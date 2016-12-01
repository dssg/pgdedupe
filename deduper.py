# -*- coding: utf-8 -*-

"""
Based on: https://github.com/datamade/dedupe-examples/tree/master/pgsql_big_dedupe_example
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

START_TIME = time.time()

def main():
    argp = ArgumentParser()
    argp.add_argument('config', nargs=1, help='Path to YAML file that specifies table and fields')
    argp.add_argument('-v', '--verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
                    )
    argp.add_argument('-d', '--db', default='/home/mbauman/psql_profile.json',
                    help='Path to YAML config file that contains host, database, user and password')
    opts = argp.parse_args()
    log_level = logging.WARNING
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose is None or opts.verbose >= 2:
        log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    with open(opts.db) as f:
        dbconfig = yaml.load(f)
    con = psy.connect(cursor_factory=psycopg2.extras.RealDictCursor, **dbconfig)

    config = load_config(opts.config[0])

    preprocess(con, config)
    deduper = train(con, config)
    create_blocking(deduper, con, config)
    clustered_dupes = cluster(deduper, con, config)
    write_results(clustered_dupes, con, config)
    # Close our database connection
    con.close()

    print('ran in', time.time() - START_TIME, 'seconds')

def load_config(filename):
    with open(filename) as f:
        return process_config(yaml.load(f))

def process_config(c):
    config = dict()
    # Required fields
    for k in ('schema', 'table', 'key', 'fields'):
        if k not in c:
            raise Exception('Key ' + k + ' must be defined in the config file')
        config[k] = c[k]
    # Optional fields
    for k, default in (('interactions', []),
                       ('threshold', 0.5),
                       ('maximum_comparisons', 100000000000),
                       ('recall', 0.90),
                       ('settings_file', 'dedup_postgres_settings'),
                       ('training_file', 'dedup_postgres_training.json')):
        config[k] = c.get(k, default)
    # Add variable names to the field definitions, defaulting to the field
    for d in config['fields']:
        if 'variable name' not in d:
            d['variable name'] = d['field']
    # Add some handy computed values for convenience
    config['all_fields'] = config['fields'] + [{'type': 'Interaction', 'interaction variables': x} for x in config['interactions']]
    columns = set([x['field'] for x in config['fields']])
    config['columns'] = ', '.join(columns)
    config['all_columns'] = ', '.join(columns | set(['_unique_id']))
    return config

def preprocess(con, config):
    c = con.cursor()

    # Do an initial first pass and merge all exact duplicates
    c.execute("""CREATE SCHEMA IF NOT EXISTS {schema}""".format(**config))

    # TODO: Make the restriction configurable
    print('creating unique entries table (this may take some time)...')
    c.execute("""DROP TABLE IF EXISTS {schema}.entries_unique""".format(**config))
    c.execute("""CREATE TABLE {schema}.entries_unique AS (
                    SELECT min({key}) as _unique_id, {columns}, array_agg({key}) as src_ids FROM {table}
                    WHERE last_name is not null AND
                        (ssn is not null
                        OR (first_name is not null AND dob is not null))
                    GROUP BY {columns})""".format(**config))
    con.commit()

# ## Training
def train(con, config):
    if False: # os.path.exists(settings_file):
        print('reading from ', config['settings_file'])
        with open(config['settings_file'], 'rb') as sf:
            return dedupe.StaticDedupe(sf, num_cores=2)
    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(config['all_fields'])

    # Named cursor runs server side with psycopg2
    cur = con.cursor('individual_select')

    cur.execute("SELECT {all_columns} FROM {schema}.entries_unique".format(**config))
    temp_d = dict((i, row) for i, row in enumerate(cur))

    deduper.sample(temp_d, 75000)
    del temp_d
    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file
    if os.path.exists(config['training_file']):
        print('reading labeled examples from ', config['training_file'])
        with open(config['training_file']) as tf:
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
    with open(config['training_file'], 'w') as tf:
        deduper.writeTraining(tf)

    # Notice our two arguments here
    #
    # `maximum_comparisons` limits the total number of comparisons that
    # a blocking rule can produce.
    #
    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(maximum_comparisons=config['maximum_comparisons'], recall=config['recall'])

    with open(config['settings_file'], 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()
    return deduper

## Blocking
def create_blocking(deduper, con, config):
    print('blocking...')
    c = con.cursor()

    # To run blocking on such a large set of data, we create a separate table
    # that contains blocking keys and record ids
    print('creating blocking_map database')
    c.execute("DROP TABLE IF EXISTS {schema}.blocking_map".format(**config))
    c.execute("CREATE TABLE {schema}.blocking_map "
              "(block_key VARCHAR(200), _unique_id INT)".format(**config)) # TODO: THIS INT needs to be dependent upon the column type of entry_id


    # If dedupe learned a Index Predicate, we have to take a pass
    # through the data and create indices.
    print('creating inverted index')

    for field in deduper.blocker.index_fields:
        c2 = con.cursor('c2')
        c2.execute("SELECT DISTINCT {0} FROM {schema}.entries_unique".format(field, **config))
        field_data = (row[field] for row in c2)
        deduper.blocker.index(field_data, field)
        c2.close()

    # Now we are ready to write our blocking map table by creating a
    # generator that yields unique `(block_key, donor_id)` tuples.
    print('writing blocking map')

    c3 = con.cursor('donor_select2')
    c3.execute("SELECT {all_columns} FROM {schema}.entries_unique".format(**config))
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
    c.copy_expert("COPY {schema}.blocking_map FROM STDIN CSV".format(**config), f)
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
    c.execute("CREATE INDEX blocking_map_key_idx ON {schema}.blocking_map (block_key)".format(**config))

    c.execute("DROP TABLE IF EXISTS {schema}.plural_key".format(**config))
    c.execute("DROP TABLE IF EXISTS {schema}.plural_block".format(**config))
    c.execute("DROP TABLE IF EXISTS {schema}.covered_blocks".format(**config))
    c.execute("DROP TABLE IF EXISTS {schema}.smaller_coverage".format(**config))

    # Many block_keys will only form blocks that contain a single
    # record. Since there are no comparisons possible withing such a
    # singleton block we can ignore them.
    logging.info("calculating {schema}.plural_key".format(**config))
    c.execute("CREATE TABLE {schema}.plural_key "
              "(block_key VARCHAR(200), "
              " block_id SERIAL PRIMARY KEY)".format(**config))

    c.execute("INSERT INTO {schema}.plural_key (block_key) "
              "SELECT block_key FROM {schema}.blocking_map "
              "GROUP BY block_key HAVING COUNT(*) > 1".format(**config))

    logging.info("creating {schema}.block_key index".format(**config))
    c.execute("CREATE UNIQUE INDEX block_key_idx ON {schema}.plural_key (block_key)".format(**config))

    logging.info("calculating {schema}.plural_block".format(**config))
    c.execute("CREATE TABLE {schema}.plural_block "
              "AS (SELECT block_id, _unique_id "
              " FROM {schema}.blocking_map INNER JOIN {schema}.plural_key "
              " USING (block_key))".format(**config))

    logging.info("adding _unique_id index and sorting index")
    c.execute("CREATE INDEX plural_block_id_idx ON {schema}.plural_block (_unique_id)".format(**config))
    c.execute("CREATE UNIQUE INDEX plural_block_block_id_id_uniq "
              " ON {schema}.plural_block (block_id, _unique_id)".format(**config))


    # To use Kolb, et.al's Redundant Free Comparison scheme, we need to
    # keep track of all the block_ids that are associated with a
    # particular donor records. We'll use PostgreSQL's string_agg function to
    # do this. This function will truncate very long lists of associated
    # ids, so we'll also increase the maximum string length to try to
    # avoid this
    # c.execute("SET group_concat_max_len = 4096")

    logging.info("creating {schema}.covered_blocks".format(**config))
    c.execute("CREATE TABLE {schema}.covered_blocks "
              " AS (SELECT _unique_id, "
              " string_agg(CAST(block_id AS TEXT), ',' ORDER BY block_id) "
              "   AS sorted_ids "
              " FROM {schema}.plural_block "
              " GROUP BY _unique_id)".format(**config))

    c.execute("CREATE UNIQUE INDEX covered_blocks_id_idx "
              "ON {schema}.covered_blocks (_unique_id)".format(**config))

    con.commit()

    # In particular, for every block of records, we need to keep
    # track of a donor records's associated block_ids that are SMALLER than
    # the current block's _unique_id. Because we ordered the ids when we did the
    # GROUP_CONCAT we can achieve this by using some string hacks.
    logging.info("creating {schema}.smaller_coverage".format(**config))
    c.execute("CREATE TABLE {schema}.smaller_coverage "
              " AS (SELECT _unique_id, block_id, "
              " TRIM(',' FROM split_part(sorted_ids, CAST(block_id AS TEXT), 1)) "
              "      AS smaller_ids "
              " FROM {schema}.plural_block INNER JOIN {schema}.covered_blocks "
              " USING (_unique_id))".format(**config))

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
                print(time.time() - START_TIME, "seconds")

        smaller_ids = row['smaller_ids']

        if smaller_ids:
            smaller_ids = lset(smaller_ids.split(','))
        else:
            smaller_ids = lset([])

        records.append((row['_unique_id'], row, smaller_ids))

    if records:
        yield records

def cluster(deduper, con, config):
    c4 = con.cursor('c4')
    c4.execute("SELECT {all_columns}, block_id, smaller_ids FROM {schema}.smaller_coverage "
               "INNER JOIN {schema}.entries_unique "
               "USING (_unique_id) "
               "ORDER BY (block_id)".format(**config))

    print('clustering...')
    return deduper.matchBlocks(candidates_gen(c4), threshold=config['threshold'])

## Writing out results
def write_results(clustered_dupes, con, config):
    c = con.cursor()
    # We now have a sequence of tuples of donor ids that dedupe believes
    # all refer to the same entity. We write this out onto an entity map
    # table
    c.execute("DROP TABLE IF EXISTS {schema}.entity_map".format(**config))

    print('creating {schema}.entity_map database'.format(**config))
    c.execute("CREATE TABLE {schema}.entity_map "
              "(_unique_id INT, canon_id INT, " #TODO: THESE INTS MUST BE DYNAMIC
              " cluster_score FLOAT, PRIMARY KEY(_unique_id))".format(**config))

    csv_file = tempfile.NamedTemporaryFile(prefix='entity_map_', delete=False,
                                           mode='w')
    csv_writer = csv.writer(csv_file)


    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for donor_id, score in zip(cluster, scores) :
            csv_writer.writerow([donor_id, cluster_id, score])

    csv_file.close()

    f = open(csv_file.name, 'r')
    c.copy_expert("COPY {schema}.entity_map FROM STDIN CSV".format(**config), f)
    f.close()

    os.remove(csv_file.name)

    con.commit()

    c.execute("CREATE INDEX head_index ON {schema}.entity_map (canon_id)".format(**config))
    con.commit()

    # Print out the number of duplicates found
    print('# duplicate sets')
    print(len(clustered_dupes))


    # ## Payoff

    # Now we can create a mapping between the canonical id and a unique integer
    # TODO: We really only need to do this if _unique_id isn't already an integer
    c.execute("DROP TABLE IF EXISTS {schema}.map".format(**config))
    c.execute("CREATE TABLE {schema}.map "
              "AS SELECT COALESCE(canon_id, _unique_id) AS canon_id,"
              "_unique_id, "
              "COALESCE(cluster_score, 1.0) AS cluster_score "
              "FROM {schema}.entity_map "
              "RIGHT JOIN {schema}.entries_unique USING(_unique_id)".format(**config))

    # Convert the canon_id to an integer id
    c.execute("DROP TABLE IF EXISTS {schema}.clusters".format(**config))
    c.execute("CREATE TABLE {schema}.clusters AS "
              "(SELECT DISTINCT canon_id FROM {schema}.map)".format(**config))
    c.execute("ALTER TABLE {schema}.clusters ADD COLUMN dedupe_id SERIAL UNIQUE".format(**config))

    # Add that cluster id back into the mapping table
    c.execute("ALTER TABLE {schema}.map ADD COLUMN dedupe_id INTEGER".format(**config))
    c.execute("UPDATE {schema}.map dst SET dedupe_id = src.dedupe_id "
              "FROM {schema}.clusters src WHERE dst.canon_id = src.canon_id".format(**config))

    # Add that integer id back to the unique_entries table
    c.execute("ALTER TABLE {schema}.entries_unique DROP COLUMN IF EXISTS dedupe_id".format(**config))
    c.execute("ALTER TABLE {schema}.entries_unique ADD COLUMN dedupe_id INTEGER".format(**config))
    c.execute("UPDATE {schema}.entries_unique u SET dedupe_id = m.dedupe_id "
              "FROM {schema}.map m WHERE u._unique_id = m._unique_id".format(**config))
    con.commit()

    # And now map it the whole way back to the entries table
    # create a mapping between the unique entries and the original entries
    c.execute("DROP TABLE IF EXISTS {schema}.unique_map".format(**config))
    c.execute("CREATE TABLE {schema}.unique_map AS ( "
              "SELECT dedupe_id, unnest(src_ids) as {key} "
              "FROM {schema}.entries_unique)".format(**config))

    con.commit()

    c.execute("ALTER TABLE {table} DROP COLUMN IF EXISTS dedupe_id".format(**config))
    c.execute("ALTER TABLE {table} ADD COLUMN dedupe_id INTEGER".format(**config))
    c.execute("UPDATE {table} u SET dedupe_id = m.dedupe_id "
              "FROM {schema}.unique_map m WHERE u.{key} = m.{key}".format(**config))


    con.commit()
    c.close()

if __name__ == '__main__':
    main()