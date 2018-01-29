import os
import csv
import tempfile
import logging
import random
import numpy
import dedupe
import importlib

from . import exact_matches


def process_options(user_config):
    """Apply defaults and transformations to given user options

    Args:
        user_config (dict) configuration options for a deduping run

    Returns: (dict) configuration options for a deduping run, with defaults and
        transformations applied
    """
    config = dict()
    # Required fields
    for k in ('schema', 'table', 'key', 'fields'):
        if k not in user_config:
            raise Exception('Key ' + k + ' must be defined in the config file')
        config[k] = user_config[k]
    # Optional fields
    for k, default in (('interactions', []),
                       ('threshold', 0.5),
                       ('recall', 0.90),
                       ('merge_exact', []),
                       ('settings_file', 'dedup_postgres_settings'),
                       ('training_file', 'dedup_postgres_training.json'),
                       ('filter_condition', '1=1'),
                       ('classifier', 'rlr.RegularizedLogisticRegression'),
                       ('hyperparameters', {}),
                       ('num_cores', None),
                       ('use_saved_model', False),
                       ('prompt_for_labels', True),
                       ('seed', None)
                       ):
        config[k] = user_config.get(k, default)
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
    config['columns'] = ', '.join(columns)
    config['all_columns'] = ', '.join(columns | set(['_unique_id']))
    return config


def preprocess(con, config):
    """Prepare the database for a deduping run

    Creates necessary schemas and database functions,
    as well as performing an exact-duplicate merge.

    When the function is done, there will be an 'entries_unique' table
    with the results of the exact-duplicate merge

    Args:
        con (psycopg2.connection)
        config (dict) configuration options for a deduping run. Expected to have defaults applied
    """
    c = con.cursor()

    # Ensure the database has the schema and required functions
    c.execute("""CREATE SCHEMA IF NOT EXISTS {schema}""".format(**config))

    # Create an intarray-like idx function (https://wiki.postgresql.org/wiki/Array_Index):
    c.execute("""CREATE OR REPLACE FUNCTION {schema}.idx(anyarray, anyelement)
                   RETURNS INT AS
                 $$
                   SELECT i FROM (
                      SELECT generate_series(array_lower($1,1),array_upper($1,1))
                   ) g(i)
                   WHERE $1[i] = $2
                   LIMIT 1;
                 $$ LANGUAGE SQL IMMUTABLE;""".format(**config))

    # Do an initial first pass and merge all exact duplicates
    c.execute("""DROP TABLE IF EXISTS {schema}.entries_unique""".format(**config))
    c.execute("""CREATE TABLE {schema}.entries_unique AS (
                    SELECT {columns}, array_agg({key}) as src_ids FROM {table}
                    WHERE ({filter_condition})
                    GROUP BY {columns})""".format(**config))
    c.execute("ALTER TABLE {schema}.entries_unique "
              " ADD COLUMN _unique_id SERIAL PRIMARY KEY".format(**config))
    con.commit()


def train(con, config):
    """Trains or retrieves a previously trained Dedupe object

    If config['use_saved_model'] is set, it will read the saved model from
    config['settings_file'] instead of training.

    If config['seed'] is set, both random.seed and numpy.random.seed will be set so
    the run will be deterministic given configuration.

    If config['prompt_for_labels'] is set, the console will be used to ask the user to
    help label examples

    Args:
        con (psycopg2.connection)
        config (dict) configuration options for a deduping run. Expected to have defaults applied

    Returns: (dedupe.Dedupe or dedupe.StaticDedupe)
    """
    if config['seed'] is not None:
        if os.environ.get('PYTHONHASHSEED', 'random') == 'random':
            raise EnvironmentError("""dedupe is only deterministic with hash randomization disabled.
                Set the PYTHONHASHSEED environment variable to a constant.""")
        random.seed(config['seed'])
        numpy.random.seed(config['seed'])
    if config['use_saved_model']:
        logging.info('reading saved model from %s', config['settings_file'])
        with open(config['settings_file'], 'rb') as sf:
            return dedupe.StaticDedupe(sf, num_cores=config['num_cores'])
    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(config['all_fields'], num_cores=config['num_cores'])

    module_name, class_name = config['classifier'].rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    deduper.classifier = cls(**config['hyperparameters'])

    # Named cursor runs server side with psycopg2
    cur = con.cursor('individual_select')

    cur.execute("""SELECT {all_columns}
                   FROM {schema}.entries_unique
                   ORDER BY _unique_id""".format(**config))
    temp_d = dict((i, row) for i, row in enumerate(cur))

    num_records = 75000
    logging.info('Creating sample of %s records', num_records)
    deduper.sample(temp_d, num_records)

    del temp_d
    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file
    if os.path.exists(config['training_file']):
        logging.info('reading labeled examples from %s', config['training_file'])
        with open(config['training_file']) as tf:
            deduper.readTraining(tf)

    if config['prompt_for_labels']:
        # ## Active learning
        logging.info('starting active labeling...')
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

    return deduper


# Blocking
def create_blocking(deduper, con, config):
    """Runs blocking on a deduper object to prepare data for matchBlocks

    Combines data from entries_unique with a trained Dedupe object. Creates blocks and writes
    them to the database.

    The following tables are created:
    blocking_map, plural_key, plural_blocks, covered_blocks, smaller_coverage

    smaller_coverage is roughly the format that deduper.matchBlocks requires. The other tables
    are considered intermediate tables.

    Args:
        deduper (dedupe.Dedupe or dedupe.StaticDedupe) A trained Dedupe object
        con (psycopg2.connection)
        config (dict) configuration options for a deduping run. Expected to have defaults applied
    """
    c = con.cursor()

    # To run blocking on such a large set of data, we create a separate table
    # that contains blocking keys and record ids
    print('creating blocking_map database')
    c.execute("DROP TABLE IF EXISTS {schema}.blocking_map".format(**config))
    c.execute("CREATE TABLE {schema}.blocking_map "
              "(block_key VARCHAR(200), _unique_id INT)".format(**config))

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
    c.execute("CREATE INDEX blocking_map_key_idx "
              " ON {schema}.blocking_map (block_key)".format(**config))

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
    c.execute("CREATE UNIQUE INDEX block_key_idx "
              " ON {schema}.plural_key (block_key)".format(**config))

    logging.info("calculating {schema}.plural_block".format(**config))
    c.execute("CREATE TABLE {schema}.plural_block "
              "AS (SELECT block_id, _unique_id "
              " FROM {schema}.blocking_map INNER JOIN {schema}.plural_key "
              " USING (block_key))".format(**config))

    logging.info("adding _unique_id index and sorting index")
    c.execute("CREATE INDEX plural_block_id_idx "
              " ON {schema}.plural_block (_unique_id)".format(**config))
    c.execute("CREATE UNIQUE INDEX plural_block_block_id_id_uniq "
              " ON {schema}.plural_block (block_id, _unique_id)".format(**config))

    # To use Kolb, et.al's Redundant Free Comparison scheme, we need to
    # keep track of all the block_ids that are associated with a
    # particular donor records.

    logging.info("creating {schema}.covered_blocks".format(**config))
    c.execute("CREATE TABLE {schema}.covered_blocks "
              " AS (SELECT _unique_id, "
              " array_agg(block_id ORDER BY block_id)  "
              "   AS sorted_ids "
              " FROM {schema}.plural_block "
              " GROUP BY _unique_id)".format(**config))

    c.execute("CREATE UNIQUE INDEX covered_blocks_id_idx "
              "ON {schema}.covered_blocks (_unique_id)".format(**config))

    con.commit()

    # In particular, for every block of records, we need to keep
    # track of a donor records's associated block_ids that are SMALLER than
    # the current block's _unique_id.
    logging.info("creating {schema}.smaller_coverage".format(**config))
    c.execute("CREATE TABLE {schema}.smaller_coverage "
              " AS (SELECT _unique_id, block_id, "
              " sorted_ids[1:({schema}.idx(sorted_ids, block_id) - 1)] "
              "      AS smaller_ids "
              " FROM {schema}.plural_block INNER JOIN {schema}.covered_blocks "
              " USING (_unique_id))".format(**config))

    con.commit()


# Clustering
def candidates_gen(result_set):
    """Yield record tuples in the form:
        (unique id, row, smaller block ids)

    Args:
        (result_set) Iterable of records, expected to have 'block_id', '_unique_id', 'smaller_ids'

    Yields:
        Records in form (unique id, row, smaller block ids)
    """
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

        smaller_ids = row['smaller_ids']

        if smaller_ids:
            smaller_ids = lset(smaller_ids)
        else:
            smaller_ids = lset([])

        records.append((row['_unique_id'], row, smaller_ids))

    if records:
        yield records


def cluster(deduper, con, config):
    """Run clustering on blocked data

    Args:
        deduper (dedupe.Dedupe or dedupe.StaticDedupe) A trained Dedupe object
        con (psycopg2.connection)
        config (dict) configuration options for a deduping run. Expected to have defaults applied

    Returns: Cluster results from dedupe.Dedupe.matchBlocks.
        Each record is in form: (cluster_id, scores)
    """
    c4 = con.cursor('c4')
    c4.execute("SELECT {all_columns}, block_id, smaller_ids FROM {schema}.smaller_coverage "
               "INNER JOIN {schema}.entries_unique "
               "USING (_unique_id) "
               "ORDER BY (block_id)".format(**config))

    return deduper.matchBlocks(candidates_gen(c4), threshold=config['threshold'])


# Writing out results
def write_results(clustered_dupes, con, config):
    """Write clustering results to the entity_map table

    Args:
        clustered_dupes (list of (cluster_id, scores) tuples)
        con (psycopg2.connection)
        config (dict) configuration options for a deduping run. Expected to have defaults applied
    """
    c = con.cursor()
    # We now have a sequence of tuples of donor ids that dedupe believes
    # all refer to the same entity. We write this out onto an entity map
    # table
    c.execute("DROP TABLE IF EXISTS {schema}.entity_map".format(**config))

    c.execute("CREATE TABLE {schema}.entity_map "
              "(_unique_id INT, canon_id INT, "
              " cluster_score FLOAT, PRIMARY KEY(_unique_id))".format(**config))

    csv_file = tempfile.NamedTemporaryFile(prefix='entity_map_', delete=False,
                                           mode='w')
    csv_writer = csv.writer(csv_file)

    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for donor_id, score in zip(cluster, scores):
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
def apply_results(con, config):
    """Combine results from entity_map table with unclustered records to create a canonical lookup

    Args:
        con (psycopg2.connection)
        config (dict) configuration options for a deduping run. Expected to have defaults applied
    """
    c = con.cursor()
    # Dedupe only cares about matched records; it doesn't have a canonical id
    # for singleton records. So we create a mapping table between *all*
    # _unique_ids to their canonical_id (or back to themselves if singletons).
    c.execute("DROP TABLE IF EXISTS {schema}.map".format(**config))
    c.execute("CREATE TABLE {schema}.map "
              "AS SELECT COALESCE(canon_id, _unique_id) AS canon_id,"
              "_unique_id, "
              "COALESCE(cluster_score, 1.0) AS cluster_score "
              "FROM {schema}.entity_map "
              "RIGHT JOIN {schema}.entries_unique USING(_unique_id)".format(**config))

    # Remove the dedupe_id column from entries if it already exists
    c.execute("ALTER TABLE {table} DROP COLUMN IF EXISTS dedupe_id".format(**config))

    # Merge clusters based upon exact matches of a subset of fields. This can
    # be done on the unique table or on the actual entries table, but it's more
    # efficient to do it now.
    available_fields = [f['field'] for f in config['fields']]
    for cols in config['merge_exact']:
        if not all(c in available_fields for c in cols):
            continue
        exact_matches.merge('{}.map'.format(config['schema']), 'canon_id',
                            '{}.entries_unique'.format(config['schema']), '_unique_id',
                            cols, config['schema'], con)

    # Add that integer id back to the unique_entries table
    c.execute("""ALTER TABLE {schema}.entries_unique
                 DROP COLUMN IF EXISTS dedupe_id""".format(**config))
    c.execute("ALTER TABLE {schema}.entries_unique ADD COLUMN dedupe_id INTEGER".format(**config))
    c.execute("UPDATE {schema}.entries_unique u SET dedupe_id = m.canon_id "
              "FROM {schema}.map m WHERE u._unique_id = m._unique_id".format(**config))
    con.commit()

    # And now map it the whole way back to the entries table
    # create a mapping between the unique entries and the original entries
    c.execute("DROP TABLE IF EXISTS {schema}.unique_map".format(**config))
    c.execute("CREATE TABLE {schema}.unique_map AS ( "
              "SELECT dedupe_id, unnest(src_ids) as {key} "
              "FROM {schema}.entries_unique)".format(**config))

    # Grab the remainder of the exact merges:
    for cols in config['merge_exact']:
        if all(c in available_fields for c in cols):
            continue
        exact_matches.merge('{}.unique_map'.format(config['schema']), 'dedupe_id',
                            config['table'], config['key'],
                            cols, config['schema'], con)
    con.commit()

    c.execute("ALTER TABLE {table} ADD COLUMN dedupe_id INTEGER".format(**config))
    c.execute("UPDATE {table} u SET dedupe_id = m.dedupe_id "
              "FROM {schema}.unique_map m WHERE u.{key} = m.{key}".format(**config))

    con.commit()
    c.close()
