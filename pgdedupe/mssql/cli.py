# -*- coding: utf-8 -*-

"""
Based on: https://github.com/datamade/dedupe-examples/tree/master/pgsql_big_dedupe_example
"""
import os
import csv
import tempfile
import time
import logging

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
	"""
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
