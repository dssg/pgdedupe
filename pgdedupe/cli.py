# -*- coding: utf-8 -*-

"""
Based on: https://github.com/datamade/dedupe-examples/tree/master/pgsql_big_dedupe_example
"""
import time
import logging

import psycopg2 as psy
import psycopg2.extras

import click

from .utils import load_config, filename_friendly_hash, create_model_definition
from .run import process_options, preprocess, train, create_blocking, cluster, write_results, apply_results

START_TIME = time.time()


@click.command()
@click.option('--config',
              help='YAML- or JSON-formatted configuration file.',
              required=True)
@click.option('--db',
              help='YAML- or JSON-formatted database connection credentials.',
              required=True)
def main(config, db, verbosity=2):
    log_level = logging.WARNING
    if verbosity == 1:
        log_level = logging.INFO
    elif verbosity is None or verbosity >= 2:
        log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    dbconfig = load_config(db)
    con = psy.connect(cursor_factory=psycopg2.extras.RealDictCursor, **dbconfig)

    config = process_options(load_config(config))

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


@click.command()
@click.option('--config',
              help='YAML- or JSON-formatted configuration file.',
              required=True)
@click.option('--db',
              help='YAML- or JSON-formatted database connection credentials.',
              required=True)
def run(config, db, verbosity=2):
    log_level = logging.WARNING
    if verbosity == 1:
        log_level = logging.INFO
    elif verbosity is None or verbosity >= 2:
        log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    dbconfig = load_config(db)
    con = psy.connect(cursor_factory=psycopg2.extras.RealDictCursor, **dbconfig)

    config = process_options(load_config(config))

    logging.info("Preprocessing...")
    preprocess(con, config)

    logging.info("Training...")
    deduper = train(con, config)

    # We need the memory-intensive objects for creating a model hash,
    # so delete them afterwards instead of within train()
    model_definition = create_model_definition(config, deduper)
    logging.info('Model definition = %s', model_definition)
    model_hash = filename_friendly_hash(model_definition)
    logging.info('Model hash = %s', model_hash)

    # free up some memory from the deduper
    deduper.cleanupTraining()

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

if __name__ == '__main__':
    run()
