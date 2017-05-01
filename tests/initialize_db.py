import click
import yaml
import os
from os import path, system


@click.command()
@click.option('--db', help='YAML-formatted database connection credentials.', required=True)
@click.option('--csv', help='CSV file to load into the database table', required=True)
def main(db, csv):
    init(db, csv)

def init(db, csv):
    # We'll shell out to `psql`, so set the environment variables for it:
    with open(db) as f:
        for k,v in yaml.load(f).items():
            os.environ['PG' + k.upper()] = str(v) if v else ""

    # And create the table from the csv file with psql
    system("""psql -c "CREATE SCHEMA IF NOT EXISTS dedupe;" """)
    system("""psql -c "DROP TABLE IF EXISTS dedupe.entries;" """)
    # system("""csvsql --no-constraints -i postgresql --table entries --db-schema dedupe < test/people.csv | psql """)
    system("""psql -c "
         CREATE TABLE "dedupe.entries" (
             uuid UUID,
             first_name VARCHAR,
             last_name VARCHAR,
             ssn VARCHAR(11),
             sex VARCHAR(1),
             dob VARCHAR(10),
             race VARCHAR,
             ethnicity VARCHAR
         );" """)
    system("""psql -c "\copy dedupe.entries FROM '{}' WITH CSV HEADER;" """.format(csv))
    system("""psql -c "alter table dedupe.entries add column entry_id SERIAL PRIMARY KEY;" """)
    system("""psql -c "alter table dedupe.entries add column full_name VARCHAR;" """)
    system("""psql -c "update dedupe.entries set full_name = first_name || ' ' || last_name;" """)

if __name__ == '__main__':
    main()
