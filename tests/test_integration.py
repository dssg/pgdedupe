# Very rudimentary integration testing
from pgdedupe import cli
from click.testing import CliRunner
import tests.generate_fake_dataset as gen

import os
import json
import psycopg2 as psy
import testing.postgresql

def test_integration():
    psql = testing.postgresql.Postgresql()
    con = psy.connect(**psql.dsn())
    c = con.cursor()

    pop = gen.create_population(2000)
    gen.create_csv(pop, 'pop.csv')

    c.execute("""
         CREATE TABLE entries (
             uuid UUID,
             first_name VARCHAR,
             last_name VARCHAR,
             ssn VARCHAR(11),
             sex VARCHAR(1),
             dob VARCHAR(10),
             race VARCHAR,
             ethnicity VARCHAR
         );""")
    con.commit()
    with open('pop.csv') as f:
        f.readline() # skip the header
        c.copy_from(f, 'entries', sep=',', null='')
    c.execute("ALTER TABLE entries ADD COLUMN entry_id SERIAL PRIMARY KEY;")
    c.execute("ALTER TABLE entries ADD COLUMN full_name VARCHAR;")
    c.execute("UPDATE entries SET full_name = first_name || ' ' || last_name;")
    con.commit()

    with open('db.json','w') as f:
        json.dump(psql.dsn(), f)
    
    runner = CliRunner()
    result = runner.invoke(cli.main, ['--config', 'config.yaml', '--db', 'db.json'])
    assert result.exit_code == 0

    c.execute("SELECT count(distinct dedupe_id) FROM entries")
    assert(c.fetchone()[0] < 4000) # Rudimentary quality check
