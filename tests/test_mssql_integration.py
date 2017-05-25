# Very rudimentary integration testing
from pgdedupe import cli
from click.testing import CliRunner
import tests.generate_fake_dataset as gen
import tests.initialize_db as initdb

import os
import yaml
import pymssql


def test_mssql_integration():
    try:
        with open('db.yaml','r') as f:
            db = yaml.load(f)
    except IOError:
        raise Exception("Please create db.yaml with all the SQL Server credentials")
    
    pop = gen.create_population(2000)
    gen.create_csv(pop, 'pop.csv')

    if 'type' in db: 
        del db['type']

    initdb.init_mssql(db, 'pop.csv')
    
    runner = CliRunner()
    result = runner.invoke(cli.main, ['--config', 'config.yaml', '--db', 'db.yaml'])
    assert result.exit_code == 0

    con = pymssql.connect(**db)
    c = con.cursor()
    c.execute("SELECT count(distinct dedupe_id) FROM dedupe.entries")
    assert(c.fetchone()[0] < 4000) # Rudimentary quality check
