# Very rudimentary integration testing
import json

from click.testing import CliRunner

from pgdedupe import cli

import pymssql

import tests.generate_fake_dataset as gen
import tests.initialize_db as initdb


def test_mssql_integration():
    # SQL Server credential for test on docker
    db = {
        'host': 'localhost',
        'password': '1@34dedupe',
        'user': 'sa',
        'port': 15433
    }

    pop = gen.create_population(2000)
    gen.create_csv(pop, 'pop.csv')

    initdb.init_mssql(db, 'pop.csv')

    with open('db.json', 'w') as f:
        db['type'] = 'mssql'
        db['database'] = 'test'
        json.dump(db, f)
        del db['type']

    runner = CliRunner()
    result = runner.invoke(cli.main, ['--config', 'config.yaml', '--db', 'db.json'])
    assert result.exit_code == 0

    con = pymssql.connect(**db)
    c = con.cursor()
    c.execute("SELECT count(distinct dedupe_id) FROM dedupe.entries")
    assert(c.fetchone()[0] < 4000)
    # Rudimentary quality check

    # Delete database test created
    con.autocommit(True)
    c.execute("USE master")
    c.execute("DROP DATABASE test")
    con.close()
