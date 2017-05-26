# -*- coding: utf-8 -*-

"""
Based on: https://github.com/datamade/dedupe-examples/tree/master/pgsql_big_dedupe_example
"""
import os
import csv
import tempfile
import time
import logging
import collections

import random
import numpy

import pymssql

import dedupe

#from . import exact_matches

START_TIME = time.time()


def mssql_main(config, db):
	con = pymssql.connect(**db)
	config = process_options(config)
	config['database'] = db['database']
	
	logging.info("Preprocessing...")
	preprocess(con, config)
	
	logging.info("Training...")
	deduper = train(con, config)
	"""
	logging.info("Creating blocking table...")
	create_blocking(deduper, con, config)

	logging.info("Clustering...")
	clustered_dupes = cluster(deduper, con, config)

	logging.info("Writing results...")
	write_results(clustered_dupes, con, config)

	logging.info("Applying results...")
	apply_results(con, config)
	"""
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
                       ('settings_file', 'dedup_postgres_settings'),
                       ('training_file', 'dedup_postgres_training.json'),
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
    config['stuff_condition'] = ' AND '.join(["t.{} COLLATE SQL_Latin1_General_CP1_CS_AS = {}.{} COLLATE SQL_Latin1_General_CP1_CS_AS".format(x, config['table'], x) for x in columns])
    # By default MS Sql Server is case insensitive
    # Need to make it insensitive as per Dedupe 
    config['case_sensitive_columns'] = ' , '.join(["{} COLLATE SQL_Latin1_General_CP1_CS_AS AS {}".format(x, x) for x in columns])
    config['columns'] = ', '.join(columns)
    config['all_columns'] = ', '.join(columns | set(['_unique_id']))
    return config


def unicode_to_str(data):
	if data == "":
		data = None
	if isinstance(data, basestring):
		return str(data)
	elif isinstance(data, collections.Mapping):
		return dict(map(unicode_to_str, data.iteritems()))
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
    c.execute("IF OBJECT_ID('{schema}.entries_unique', 'U') IS NOT NULL DROP TABLE {schema}.entries_unique".format(**config))
    c.execute("""SELECT DISTINCT {case_sensitive_columns} INTO {schema}.entries_unique
                    FROM {table} 
                    WHERE ({filter_condition})""".format(**config))

    c.execute("ALTER TABLE {schema}.entries_unique "
              " ADD _unique_id INT IDENTITY(1,1) PRIMARY KEY".format(**config))

    c.execute("IF OBJECT_ID('{schema}.entries_src_ids', 'U') IS NOT NULL DROP TABLE {schema}.entries_src_ids".format(**config))
    c.execute("""SELECT t._unique_id, {table}.{key}
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

    cur = con.cursor(as_dict = True)

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

