# -*- coding: utf-8 -*-

"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.
With such a large set of input data, we cannot store all the comparisons
we need to make in memory. Instead, we will read the pairs on demand
from the PostgresSQL database.
__Note:__ You will need to run `python pgsql_big_dedupe_example_init_db.py`
before running this script.
For smaller datasets (<10,000), see our
[csv_example](http://datamade.github.io/dedupe-examples/docs/csv_example.html)
"""
import os
import csv
import tempfile
import time
import logging
import locale
import json
from argparse import ArgumentParser

import yaml
import psycopg2 as psy
import psycopg2.extras

import dedupe

argp = ArgumentParser()
argp.add_argument('config', nargs=1, help='Path to YAML file that specifies table and fields')
argp.add_argument('-v', '--verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
argp.add_argument('-d', '--db', default='/home/mbauman/psql_profile.json',
                help='Path to JSON config file that contains host, database, user and password')
opts = argp.parse_args()
log_level = logging.WARNING 
if opts.verbose == 1:
    log_level = logging.INFO
elif opts.verbose is None or opts.verbose >= 2:
    log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

settings_file = 'dedup_postgres_settings'
training_file = 'dedup_postgres_training.json'

start_time = time.time()
with open(opts.db) as f:
    dbconfig = yaml.load(f)
con = psy.connect(cursor_factory=psycopg2.extras.RealDictCursor, **dbconfig)

c = con.cursor()

with open(opts.config[0]) as f:
    config = yaml.load(f)

# Add variable names to the field definitions, defaulting to the field
for d in config['fields']:
    if 'variable name' not in d:
        d['variable name'] = d['field']

fields = config['fields'] + [{'type': 'Interaction', 'interaction variables': x} for x in config['interactions']]
columns = set([x['field'] for x in config['fields']])
all_columns = columns | set(['_unique_id'])

# Do an initial first pass and remove all columns
# TODO: Make the restriction configurable
print('creating unique entries table (this may take some time)...')
c.execute("""DROP TABLE IF EXISTS dedupe.entries_unique""")
c.execute("""CREATE TABLE dedupe.entries_unique AS (
                SELECT min({0}) as _unique_id, {1}, array_agg({0}) as src_ids FROM {2}
                WHERE last_name is not null AND
                    (ssn is not null
                    OR (first_name is not null AND dob is not null))
                GROUP BY {1})""".format(
                    config['key'],
                    ', '.join(columns),
                    config['table']))
con.commit()

# ## Training

if False: # os.path.exists(settings_file):
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=2)
else:
    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # Named cursor runs server side with psycopg2
    cur = con.cursor('individual_select')

    cur.execute("SELECT {} FROM dedupe.entries_unique".format(', '.join(all_columns)))
    temp_d = dict((i, row) for i, row in enumerate(cur))

    deduper.sample(temp_d, 75000)
    del temp_d
    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            deduper.readTraining(tf)

    # ## Active learning

    print('starting active labeling...')
    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    dedupe.convenience.consoleLabel(deduper)
    # When finished, save our labeled, training pairs to disk
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    # Notice our two arguments here
    #
    # `maximum_comparisons` limits the total number of comparisons that
    # a blocking rule can produce.
    #
    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(maximum_comparisons=100000000000, recall=0.90)

    with open(settings_file, 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()

## Blocking
print('blocking...')

# To run blocking on such a large set of data, we create a separate table
# that contains blocking keys and record ids
print('creating blocking_map database')
c.execute("DROP TABLE IF EXISTS dedupe.blocking_map")
c.execute("CREATE TABLE dedupe.blocking_map "
          "(block_key VARCHAR(200), _unique_id INT)") # TODO: THIS INT needs to be dependent upon the column type of entry_id


# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

for field in deduper.blocker.index_fields:
    c2 = con.cursor('c2')
    c2.execute("SELECT DISTINCT {} FROM dedupe.entries_unique".format(field))
    field_data = (row[field] for row in c2)
    deduper.blocker.index(field_data, field)
    c2.close()

# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(block_key, donor_id)` tuples.
print('writing blocking map')

c3 = con.cursor('donor_select2')
c3.execute("SELECT {} FROM dedupe.entries_unique".format(', '.join(all_columns)))
full_data = ((row['_unique_id'], row) for row in c3)
b_data = deduper.blocker(full_data)

# Write out blocking map to CSV so we can quickly load in with
# Postgres COPY
csv_file = tempfile.NamedTemporaryFile(prefix='blocks_', delete=False, mode='w')
csv_writer = csv.writer(csv_file)
csv_writer.writerows(b_data)
c3.close()
csv_file.close()

f = open(csv_file.name, 'r')
c.copy_expert("COPY dedupe.blocking_map FROM STDIN CSV", f)
f.close()

os.remove(csv_file.name)

con.commit()


# Remove blocks that contain only one record, sort by block key and
# donor, key and index blocking map.
#
# These steps, particularly the sorting will let us quickly create
# blocks of data for comparison
print('prepare blocking table. this will probably take a while ...')

logging.info("indexing block_key")
c.execute("CREATE INDEX blocking_map_key_idx ON dedupe.blocking_map (block_key)")

c.execute("DROP TABLE IF EXISTS dedupe.plural_key")
c.execute("DROP TABLE IF EXISTS dedupe.plural_block")
c.execute("DROP TABLE IF EXISTS dedupe.covered_blocks")
c.execute("DROP TABLE IF EXISTS dedupe.smaller_coverage")

# Many block_keys will only form blocks that contain a single
# record. Since there are no comparisons possible withing such a
# singleton block we can ignore them.
logging.info("calculating dedupe.plural_key")
c.execute("CREATE TABLE dedupe.plural_key "
          "(block_key VARCHAR(200), "
          " block_id SERIAL PRIMARY KEY)")

c.execute("INSERT INTO dedupe.plural_key (block_key) "
          "SELECT block_key FROM dedupe.blocking_map "
          "GROUP BY block_key HAVING COUNT(*) > 1")

logging.info("creating dedupe.block_key index")
c.execute("CREATE UNIQUE INDEX block_key_idx ON dedupe.plural_key (block_key)")

logging.info("calculating dedupe.plural_block")
c.execute("CREATE TABLE dedupe.plural_block "
          "AS (SELECT block_id, _unique_id "
          " FROM dedupe.blocking_map INNER JOIN dedupe.plural_key "
          " USING (block_key))")

logging.info("adding _unique_id index and sorting index")
c.execute("CREATE INDEX plural_block_id_idx ON dedupe.plural_block (_unique_id)")
c.execute("CREATE UNIQUE INDEX plural_block_block_id_id_uniq "
          " ON dedupe.plural_block (block_id, _unique_id)")


# To use Kolb, et.al's Redundant Free Comparison scheme, we need to
# keep track of all the block_ids that are associated with a
# particular donor records. We'll use PostgreSQL's string_agg function to
# do this. This function will truncate very long lists of associated
# ids, so we'll also increase the maximum string length to try to
# avoid this
# c.execute("SET group_concat_max_len = 4096")

logging.info("creating dedupe.covered_blocks")
c.execute("CREATE TABLE dedupe.covered_blocks "
          "AS (SELECT _unique_id, "
          " string_agg(CAST(block_id AS TEXT), ',' ORDER BY block_id) "
          "   AS sorted_ids "
          " FROM dedupe.plural_block "
          " GROUP BY _unique_id)")

c.execute("CREATE UNIQUE INDEX covered_blocks_id_idx "
          "ON dedupe.covered_blocks (_unique_id)")

con.commit()

# In particular, for every block of records, we need to keep
# track of a donor records's associated block_ids that are SMALLER than
# the current block's _unique_id. Because we ordered the ids when we did the
# GROUP_CONCAT we can achieve this by using some string hacks.
logging.info("creating dedupe.smaller_coverage")
c.execute("CREATE TABLE dedupe.smaller_coverage "
          "AS (SELECT _unique_id, block_id, "
          " TRIM(',' FROM split_part(sorted_ids, CAST(block_id AS TEXT), 1)) "
          "      AS smaller_ids "
          " FROM dedupe.plural_block INNER JOIN dedupe.covered_blocks "
          " USING (_unique_id))")

con.commit()


## Clustering

def candidates_gen(result_set):
    lset = set

    block_id = None
    records = []
    i = 0
    for row in result_set:
        if row['block_id'] != block_id:
            if records:
                yield records

            block_id = row['block_id']
            records = []
            i += 1

            if i % 10000 == 0:
                print(i, "blocks")
                print(time.time() - start_time, "seconds")

        smaller_ids = row['smaller_ids']

        if smaller_ids:
            smaller_ids = lset(smaller_ids.split(','))
        else:
            smaller_ids = lset([])

        records.append((row['_unique_id'], row, smaller_ids))

    if records:
        yield records

c4 = con.cursor('c4')
c4.execute("SELECT {}, block_id, smaller_ids FROM dedupe.smaller_coverage "
           "INNER JOIN dedupe.entries_unique "
           "USING (_unique_id) "
           "ORDER BY (block_id)".format(', '.join(all_columns)))

print('clustering...')
clustered_dupes = deduper.matchBlocks(candidates_gen(c4),
                                      threshold=0.5)

## Writing out results

# We now have a sequence of tuples of donor ids that dedupe believes
# all refer to the same entity. We write this out onto an entity map
# table
c.execute("DROP TABLE IF EXISTS dedupe.entity_map")

print('creating dedupe.entity_map database')
c.execute("CREATE TABLE dedupe.entity_map "
          "(_unique_id INT, canon_id INT, " #TODO: THESE INTS MUST BE DYNAMIC
          " cluster_score FLOAT, PRIMARY KEY(_unique_id))")

csv_file = tempfile.NamedTemporaryFile(prefix='entity_map_', delete=False,
                                       mode='w')
csv_writer = csv.writer(csv_file)


for cluster, scores in clustered_dupes:
    cluster_id = cluster[0]
    for donor_id, score in zip(cluster, scores) :
        csv_writer.writerow([donor_id, cluster_id, score])

c4.close()
csv_file.close()

f = open(csv_file.name, 'r')
c.copy_expert("COPY dedupe.entity_map FROM STDIN CSV", f)
f.close()

os.remove(csv_file.name)

con.commit()

c.execute("CREATE INDEX head_index ON dedupe.entity_map (canon_id)")
con.commit()

# Print out the number of duplicates found
print('# duplicate sets')
print(len(clustered_dupes))


# ## Payoff

# Now we can create a mapping between the canonical id and a unique integer
# TODO: We really only need to do this if _unique_id isn't already an integer
c.execute("DROP TABLE IF EXISTS dedupe.map")
c.execute("CREATE TABLE dedupe.map "
          "AS SELECT COALESCE(canon_id, _unique_id) AS canon_id,"
          "_unique_id, "
          "COALESCE(cluster_score, 1.0) AS cluster_score "
          "FROM dedupe.entity_map "
          "RIGHT JOIN dedupe.entries_unique USING(_unique_id)")

# Convert the canon_id to an integer id
c.execute("DROP TABLE IF EXISTS dedupe.clusters")
c.execute("CREATE TABLE dedupe.clusters AS "
          "(SELECT DISTINCT canon_id FROM dedupe.map)")
c.execute("ALTER TABLE dedupe.clusters ADD COLUMN dedupe_id SERIAL UNIQUE")

# Add that cluster id back into the mapping table
c.execute("ALTER TABLE dedupe.map ADD COLUMN dedupe_id INTEGER")
c.execute("UPDATE dedupe.map dst SET dedupe_id = src.dedupe_id "
          "FROM dedupe.clusters src WHERE dst.canon_id = src.canon_id")

# Add that integer id back to the unique_entries table
c.execute("ALTER TABLE dedupe.entries_unique DROP COLUMN IF EXISTS dedupe_id")
c.execute("ALTER TABLE dedupe.entries_unique ADD COLUMN dedupe_id INTEGER")
c.execute("UPDATE dedupe.entries_unique u SET dedupe_id = m.dedupe_id "
          "FROM dedupe.map m WHERE u._unique_id = m._unique_id")
con.commit()

# And now map it the whole way back to the entries table
# create a mapping between the unique entries and the original entries
c.execute("DROP TABLE IF EXISTS dedupe.unique_map")
c.execute("CREATE TABLE dedupe.unique_map AS ("
          "SELECT dedupe_id, unnest(src_ids) as {}"
          "FROM dedupe.entries_unique)".format(config['key']))
          
con.commit()

c.execute("ALTER TABLE {} DROP COLUMN IF EXISTS dedupe_id".format(config['table']))
c.execute("ALTER TABLE {} ADD COLUMN dedupe_id INTEGER".format(config['table']))
c.execute("UPDATE {0} u SET dedupe_id = m.dedupe_id "
          "FROM dedupe.unique_map m WHERE u.{1} = m.{1}".format(config['table'], config['key']))


con.commit()

# Close our database connection
c.close()
con.close()

print('ran in', time.time() - start_time, 'seconds')
