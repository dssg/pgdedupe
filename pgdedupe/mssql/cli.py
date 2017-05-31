# -*- coding: utf-8 -*-

"""
Based on: https://github.com/datamade/dedupe-examples/tree/master/pgsql_big_dedupe_example
"""
import collections
import csv
import logging
import os
import random
import sys
import tempfile
import time

import dedupe

import numpy

import pymssql

sys.path.append(os.path.abspath('pgdedupe'))
from pgdedupe import exact_matches

START_TIME = time.time()
PYTHON_VERSION = sys.version_info[0]


def mssql_main(config, db):
    con = pymssql.connect(**db)
    config = process_options(config)
    config['database'] = db['database']

    logging.info("Preprocessing...")
    preprocess(con, config)

    logging.info("Training...")
    deduper = train(con, config)

    logging.info("Creating blocking table...")
    create_blocking(deduper, con, config)

    logging.info("Clustering...")
    clustered_dupes = cluster(deduper, con, config)

    logging.info("Writing results...")
    write_results(clustered_dupes, con, config)

    logging.info("Applying results...")
    apply_results(con, config)

    # Close our database connection
    con.close()

    print('ran in', time.time() - START_TIME, 'seconds')


def process_options(c):
    config = dict()
    # Required fields
    for k in ('schema', 'table', 'key', 'fields'):
        if k not in c:
            raise Exception('Key ' + k + ' must be defined in the config file')
        config[k] = c[k]
    # Optional fields
    for k, default in (('interactions', []),
                       ('threshold', 0.5),
                       ('recall', 0.90),
                       ('merge_exact', []),
                       ('settings_file', 'dedup_mssql_settings'),
                       ('training_file', 'dedup_mssql_training.json'),
                       ('filter_condition', '1=1'),
                       ('num_cores', None),
                       ('use_saved_model', False),
                       ('prompt_for_labels', True),
                       ('seed', None)
                       ):
        config[k] = c.get(k, default)
    # Ensure that the merge_exact list is a list of lists
    if type(config['merge_exact']) is not list:
        raise Exception('merge_exact must be a list of columns')
    if len(config['merge_exact']) > 0 and type(config['merge_exact'][0]) is not list:
        config['merge_exact'] = [config['merge_exact']]
    # Add variable names to the field definitions, defaulting to the field
    for d in config['fields']:
        if 'variable name' not in d:
            d['variable name'] = d['field']
    # Add some handy computed values for convenience
    config['all_fields'] = config['fields'] + [
        {'type': 'Interaction', 'interaction variables': x} for x in config['interactions']]
    columns = set([x['field'] for x in config['fields']])
    # Used to nested sub-queries
    config['stuff_condition'] = ' AND '.join(
        ["""(t.{name} COLLATE SQL_Latin1_General_CP1_CS_AS = {table}.{name}
        COLLATE SQL_Latin1_General_CP1_CS_AS
        OR CHECKSUM(t.{name}) = CHECKSUM({table}.{name}))""".format(
         name=x, table=config['table']) for x in columns])
    # By default MS Sql Server is case insensitive
    # Need to make it insensitive as per Dedupe
    config['case_sensitive_columns'] = ' , '.join(["""{} COLLATE
        SQL_Latin1_General_CP1_CS_AS AS {}""".format(x, x) for x in columns])
    config['columns'] = ', '.join(columns)
    config['all_columns'] = ', '.join(columns | set(['_unique_id']))
    return config


def unicode_to_str(data):
    if data == "":
        data = None
    if PYTHON_VERSION < 3:
        if isinstance(data, basestring):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(unicode_to_str, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(unicode_to_str, data))
        else:
            return data
    else:
        if isinstance(data, str):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(unicode_to_str, data.items()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(unicode_to_str, data))
        else:
            return data


def preprocess(con, config):
    c = con.cursor()

    # Ensure the database has the schema and required functions
    c.execute("""IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
                BEGIN
                EXEC('CREATE SCHEMA {schema}')
                END""".format(**config))
    """
    # Create an intarray-like idx function (https://wiki.postgresql.org/wiki/Array_Index):
    c.execute(""CREATE OR REPLACE FUNCTION {schema}.idx(anyarray, anyelement)
                   RETURNS INT A
                 $$
                   SELECT i FROM (
                      SELECT generate_series(array_lower($1,1),array_upper($1,1))
                   ) g(i)
                   WHERE $1[i] = $2
                   LIMIT 1;
                 $$ LANGUAGE SQL IMMUTABLE;".format(**config))
    """
    # Do an initial first pass and merge all exact duplicates
    c.execute("IF OBJECT_ID('{schema}.entries_unique', 'U') IS NOT NULL "
              "DROP TABLE {schema}.entries_unique".format(**config))
    c.execute("""SELECT DISTINCT {case_sensitive_columns} INTO {schema}.entries_unique
                    FROM {table}
                    WHERE ({filter_condition})""".format(**config))

    c.execute("ALTER TABLE {schema}.entries_unique "
              " ADD _unique_id INT IDENTITY(1,1) PRIMARY KEY".format(**config))

    c.execute("IF OBJECT_ID('{schema}.entries_src_ids', 'U') IS NOT NULL "
              "DROP TABLE {schema}.entries_src_ids".format(**config))
    c.execute("""SELECT t._unique_id, {table}.{key} INTO {schema}.entries_src_ids
                    FROM {schema}.entries_unique as t, {table}
                    WHERE {stuff_condition}""".format(**config))

    con.commit()


# Training
def train(con, config):
    if config['seed'] is not None:
        if os.environ.get('PYTHONHASHSEED', 'random') == 'random':
            logging.warn("""dedupe is only deterministic with hash randomization disabled.
                            Set the PYTHONHASHSEED environment variable to a constant.""")
        random.seed(config['seed'])
        numpy.random.seed(config['seed'])
    if config['use_saved_model']:
        print('reading from ', config['settings_file'])
        with open(config['settings_file'], 'rb') as sf:
            return dedupe.StaticDedupe(sf, num_cores=config['num_cores'])
    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(config['all_fields'], num_cores=config['num_cores'])

    cur = con.cursor(as_dict=True)

    cur.execute("""SELECT {all_columns}
                   FROM {schema}.entries_unique
                   ORDER BY _unique_id""".format(**config))
    temp_d = dict((i, unicode_to_str(row)) for i, row in enumerate(cur))

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

    if config['prompt_for_labels']:
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

    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(recall=config['recall'])

    with open(config['settings_file'], 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()
    return deduper


# Blocking
def create_blocking(deduper, con, config):
    c = con.cursor()

    # To run blocking on such a large set of data, we create a separate table
    # that contains blocking keys and record ids
    print('creating blocking_map database')
    c.execute("IF OBJECT_ID('{schema}.blocking_map', 'U') IS NOT NULL "
              "DROP TABLE {schema}.blocking_map".format(**config))
    c.execute("CREATE TABLE {schema}.blocking_map "
              "(block_key VARCHAR(200), _unique_id INT)".format(**config))  # TODO: THIS INT...
    # ... needs to be dependent upon the column type of entry_id

    # If dedupe learned a Index Predicate, we have to take a pass
    # through the data and create indices.
    print('creating inverted index')

    for field in deduper.blocker.index_fields:
        c2 = con.cursor(as_dict=True)
        c2.execute("SELECT DISTINCT {0} FROM {schema}.entries_unique".format(field, **config))
        field_data = (unicode_to_str(row)[field] for row in c2)
        deduper.blocker.index(field_data, field)
        c2.close()

    # Now we are ready to write our blocking map table by creating a
    # generator that yields unique `(block_key, donor_id)` tuples.
    print('writing blocking map')

    c3 = con.cursor(as_dict=True)
    c3.execute("SELECT {all_columns} FROM {schema}.entries_unique".format(**config))
    full_data = ((row['_unique_id'], unicode_to_str(row)) for row in c3)
    b_data = deduper.blocker(full_data)

    # Write out blocking map to CSV so we can quickly load in with
    csv_file = tempfile.NamedTemporaryFile(prefix='blocks_', delete=False, mode='w')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerows(b_data)
    c3.close()
    csv_file.close()

    # write from csv to SQL
    with open(csv_file.name, 'r') as f:
        reader = csv.reader(f)
        data = next(reader)
        query = "INSERT INTO {schema}.blocking_map".format(**config)
        query = query + " VALUES ({0})".format(','.join(['%s'] * len(data)))
        cc = con.cursor()
        cc.execute(query, tuple(data))
        for data in reader:
            cc.execute(query, tuple(data))

    os.remove(csv_file.name)
    con.commit()

    # Remove blocks that contain only one record, sort by block key and
    # donor, key and index blocking map.
    #
    # These steps, particularly the sorting will let us quickly create
    # blocks of data for comparison
    print('prepare blocking table. this will probably take a while ...')

    logging.info("indexing block_key")
    c.execute("CREATE INDEX blocking_map_key_idx "
              " ON {schema}.blocking_map (block_key)".format(**config))

    c.execute("IF OBJECT_ID('{schema""}.plural_key', 'U') IS NOT NULL "
              "DROP TABLE {schema}.plural_key".format(**config))
    c.execute("IF OBJECT_ID('{schema}.plural_block', 'U') IS NOT NULL "
              "DROP TABLE {schema}.plural_block".format(**config))
    c.execute("IF OBJECT_ID('{schema}.covered_blocks', 'U') IS NOT NULL "
              "DROP TABLE {schema}.covered_blocks".format(**config))
    c.execute("IF OBJECT_ID('{schema}.smaller_coverage', 'U') IS NOT NULL "
              "DROP TABLE {schema}.smaller_coverage".format(**config))

    # Many block_keys will only form blocks that contain a single
    # record. Since there are no comparisons possible withing such a
    # singleton block we can ignore them.
    logging.info("calculating {schema}.plural_key".format(**config))
    c.execute("CREATE TABLE {schema}.plural_key "
              "(block_key VARCHAR(200), "
              " block_id INT IDENTITY(1,1) PRIMARY KEY)".format(**config))

    c.execute("INSERT INTO {schema}.plural_key (block_key) "
              "SELECT block_key FROM {schema}.blocking_map "
              "GROUP BY block_key HAVING COUNT(*) > 1".format(**config))

    logging.info("creating {schema}.block_key index".format(**config))
    c.execute("CREATE UNIQUE INDEX block_key_idx "
              " ON {schema}.plural_key (block_key)".format(**config))

    logging.info("calculating {schema}.plural_block".format(**config))
    c.execute("SELECT block_id, _unique_id INTO {schema}.plural_block"
              " FROM {schema}.blocking_map AS bm INNER JOIN {schema}.plural_key AS pk"
              " ON bm.block_key = pk.block_key".format(**config))

    logging.info("adding _unique_id index and sorting index")
    c.execute("CREATE INDEX plural_block_id_idx "
              " ON {schema}.plural_block (_unique_id)".format(**config))
    c.execute("CREATE UNIQUE INDEX plural_block_block_id_id_uniq "
              " ON {schema}.plural_block (block_id, _unique_id)".format(**config))

    # To use Kolb, et.al's Redundant Free Comparison scheme, we need to
    # keep track of all the block_ids that are associated with a
    # particular donor records.
    # In particular, for every block of records, we need to keep
    # track of a donor records's associated block_ids that are SMALLER than
    # the current block's _unique_id.
    logging.info("creating {schema}.smaller_coverage".format(**config))
    c.execute("SELECT pb._unique_id, pbs.block_id,"
              " STUFF("
              "(SELECT CONVERT(varchar(100), block_id) + ','"
              " FROM {schema}.plural_block"
              " WHERE block_id < pbs.block_id AND _unique_id = pbs._unique_id"
              " FOR XML PATH('')),1,0,''"
              ") AS smaller_ids"
              " INTO {schema}.smaller_coverage"
              " FROM {schema}.plural_block AS pb INNER JOIN {schema}.plural_block AS pbs"
              " ON pb._unique_id = pbs._unique_id".format(**config))

    con.commit()


# Clustering
def candidates_gen(result_set):
    lset = set

    block_id = None
    records = []
    i = 0
    for row in result_set:
        row = unicode_to_str(row)
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
            smaller_ids = lset([int(x) for x in smaller_ids.split(',')[:-1]])
        else:
            smaller_ids = lset([])

        records.append((row['_unique_id'], row, smaller_ids))

    if records:
        yield records


def cluster(deduper, con, config):
    c4 = con.cursor(as_dict=True)
    # Doing so to remove "Ambiguous column name '_unique_id' error
    all_columns_unambiguous = config['all_columns'].replace('_unique_id',
                                                            '{schema}.smaller_coverage._unique_id'
                                                            .format(**config))
    c4.execute("SELECT " + all_columns_unambiguous +
               ", block_id, smaller_ids"
               " FROM {schema}.smaller_coverage"
               " INNER JOIN {schema}.entries_unique"
               " ON {schema}.smaller_coverage._unique_id = {schema}.entries_unique._unique_id"
               " ORDER BY block_id".format(**config))

    return deduper.matchBlocks(candidates_gen(c4), threshold=config['threshold'])


# Writing out results
def write_results(clustered_dupes, con, config):
    c = con.cursor(as_dict=True)
    # We now have a sequence of tuples of donor ids that dedupe believes
    # all refer to the same entity. We write this out onto an entity map
    # table
    c.execute("IF OBJECT_ID('{schema}.entity_map', 'U') IS NOT NULL "
              "DROP TABLE {schema}.entity_map""".format(**config))
    c.execute("CREATE TABLE {schema}.entity_map "
              "(_unique_id INT, canon_id INT, "  # TODO: THESE INTS MUST BE DYNAMIC
              " cluster_score FLOAT, PRIMARY KEY(_unique_id))".format(**config))

    csv_file = tempfile.NamedTemporaryFile(prefix='entity_map_', delete=False,
                                           mode='w')
    csv_writer = csv.writer(csv_file)

    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for donor_id, score in zip(cluster, scores):
            csv_writer.writerow([donor_id, cluster_id, score])

    csv_file.close()

    # write from csv to SQL
    with open(csv_file.name, 'r') as f:
        reader = csv.reader(f)
        data = next(reader)
        query = "INSERT INTO {schema}.entity_map".format(**config)
        query = query + " VALUES ({0})".format(','.join(['%s'] * len(data)))
        cc = con.cursor()
        cc.execute(query, tuple(data))
        for data in reader:
            cc.execute(query, tuple(data))

    os.remove(csv_file.name)

    con.commit()

    c.execute("CREATE INDEX head_index ON {schema}.entity_map (canon_id)".format(**config))
    con.commit()

    # Print out the number of duplicates found
    print('# duplicate sets')
    print(len(clustered_dupes))


# ## Payoff
def apply_results(con, config):
    c = con.cursor()
    # Dedupe only cares about matched records; it doesn't have a canonical id
    # for singleton records. So we create a mapping table between *all*
    # _unique_ids to their canonical_id (or back to themselves if singletons).
    c.execute("IF OBJECT_ID('{schema}.map', 'U') IS NOT NULL "
              "DROP TABLE {schema}.map".format(**config))
    c.execute("SELECT COALESCE(canon_id, ems._unique_id) AS canon_id,"
              "ems._unique_id, "
              "COALESCE(cluster_score, 1.0) AS cluster_score "
              "INTO {schema}.map "
              "FROM {schema}.entity_map AS em "
              "RIGHT JOIN {schema}.entries_unique AS ems "
              "ON em._unique_id = ems._unique_id".format(**config))

    # Remove the dedupe_id column from entries if it already exists
    c.execute("IF (SELECT COL_LENGTH('{table}', 'dedupe_id')) IS NOT NULL "
              "ALTER TABLE {table} DROP COLUMN dedupe_id".format(**config))

    # Merge clusters based upon exact matches of a subset of fields. This can
    # be done on the unique table or on the actual entries table, but it's more
    # efficient to do it now.
    available_fields = [f['field'] for f in config['fields']]
    for cols in config['merge_exact']:
        if not all(c in available_fields for c in cols):
            continue
        exact_matches.merge_mssql('{}.map'.format(config['schema']), 'canon_id',
                                  '{}.entries_unique'.format(config['schema']), '_unique_id',
                                  cols, config['schema'], con)

    # Add that integer id back to the unique_entries table
    c.execute("IF (SELECT COL_LENGTH('{schema}.entries_unique', 'dedupe_id')) IS NOT NULL "
              "ALTER TABLE {schema}.entries_unique DROP COLUMN dedupe_id".format(**config))
    c.execute("ALTER TABLE {schema}.entries_unique ADD dedupe_id INT".format(**config))
    c.execute("UPDATE {schema}.entries_unique SET dedupe_id = m.canon_id "
              "FROM {schema}.map AS m "
              "WHERE {schema}.entries_unique._unique_id = m._unique_id".format(**config))
    con.commit()

    # And now map it the whole way back to the entries table
    # create a mapping between the unique entries and the original entries
    c.execute("IF OBJECT_ID('{schema}.unique_map', 'U') IS NOT NULL "
              "DROP TABLE {schema}.unique_map".format(**config))
    c.execute("SELECT eu.dedupe_id, {schema}.entries_src_ids.{key} as {key} "
              "INTO {schema}.unique_map "
              "FROM {schema}.entries_unique AS eu INNER JOIN {schema}.entries_src_ids "
              "ON eu._unique_id = {schema}.entries_src_ids._unique_id".format(**config))

    # Grab the remainder of the exact merges:
    for cols in config['merge_exact']:
        if all(c in available_fields for c in cols):
            continue
        exact_matches.merge_mssql('{}.unique_map'.format(config['schema']), 'dedupe_id',
                                  config['table'], config['key'],
                                  cols, config['schema'], con)

    c.execute("ALTER TABLE {table} ADD dedupe_id INT".format(**config))
    c.execute("UPDATE {table} SET dedupe_id = t.dedupe_id "
              "FROM {schema}.entries_unique AS t WHERE {stuff_condition}".format(**config))
    c.execute("UPDATE {table} SET dedupe_id = m.dedupe_id "
              "FROM {schema}.unique_map AS m WHERE {table}.{key} = m.{key}".format(**config))

    con.commit()
    c.close()
