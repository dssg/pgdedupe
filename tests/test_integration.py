# Very rudimentary integration testing
from pgdedupe import cli
from click.testing import CliRunner
import tests.generate_fake_dataset as gen
import tests.initialize_db as initdb

import os
import yaml
import psycopg2 as psy
import testing.postgresql


def test_integration():
    psql = testing.postgresql.Postgresql()
    with open('db.yaml','w') as f:
        yaml.dump(psql.dsn(), f)

    pop = gen.create_population(2000)
    gen.create_csv(pop, 'pop.csv')

    initdb.init('db.yaml', 'pop.csv')
    
    runner = CliRunner()
    result = runner.invoke(cli.main, ['--config', 'config.yaml', '--db', 'db.yaml'])
    assert result.exit_code == 0

    con = psy.connect(**psql.dsn())
    c = con.cursor()
    c.execute("SELECT count(distinct dedupe_id) FROM dedupe.entries")
    assert(c.fetchone()[0] < 4000) # Rudimentary quality check
