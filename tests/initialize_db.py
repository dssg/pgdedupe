
import csv
import os
from os import system

import click

import pymssql

import yaml


@click.command()
@click.option('--db', help='YAML-formatted database connection credentials.', required=True)
@click.option('--csv', help='CSV file to load into the database table', required=True)
def main(db, csv):

    with open(db) as f:
        db_config = yaml.load(f)
        for k, v in db_config.items():
            os.environ['PG' + k.upper()] = str(v) if v else ""

    if db_config.get('type', 'postgres') == 'mssql':
        del db_config['type']
        init_mssql(db_config, csv)
    else:
        init(db, csv)


def init(db, csv):
    # We'll shell out to `psql`, so set the environment variables for it:
    # And create the table from the csv file with psql
    with open(db) as f:
        for k, v in yaml.load(f).items():
            os.environ['PG' + k.upper()] = str(v) if v else ""

    system("""psql -c "CREATE SCHEMA IF NOT EXISTS dedupe;" """)
    system("""psql -c "DROP TABLE IF EXISTS dedupe.entries;" """)

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


def init_mssql(db, csv_file):
    # Connect to database and use cursor to insert csv data to table
    con = pymssql.connect(**db)

    # Create DB for testing. Do not nned to install sql-tools in Travis
    con.autocommit(True)
    c = con.cursor()
    c.execute("IF NOT EXISTS(select * from sys.databases where name='test') CREATE DATABASE test")
    c.close()
    con.close()

    db['database'] = 'test'
    con = pymssql.connect(**db)
    c = con.cursor()
    c.execute("""IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dedupe')
            BEGIN
            EXEC('CREATE SCHEMA dedupe')
            END""")

    c.execute("IF OBJECT_ID('dedupe.entries', 'U') IS NOT NULL DROP TABLE dedupe.entries;")

    # Lazy definition for the table; Can be made more precise
    c.execute("""CREATE TABLE dedupe.entries (
             uuid CHAR(38) NOT NULL,
             first_name NVARCHAR(100),
             last_name NVARCHAR(100),
             ssn NVARCHAR(11),
             sex NVARCHAR(1),
             dob NVARCHAR(10),
             race NVARCHAR(100),
             ethnicity NVARCHAR(100)
         ) """)

    con.commit()

    # Read CSV
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)
        query = """INSERT INTO dedupe.entries({0})
        VALUES ({1})""".format(', '.join(columns), ', '.join(['%s'] * len(columns)))
        for data in reader:
            c.execute(query, tuple(data))
            con.commit()

    c.execute("""ALTER TABLE dedupe.entries ADD entry_id INT IDENTITY(1,1) PRIMARY KEY""")
    c.execute("""ALTER TABLE dedupe.entries ADD full_name VARCHAR(100)""")
    c.execute("""UPDATE dedupe.entries SET full_name = first_name + ' ' + last_name""")
    c.execute("""UPDATE dedupe.entries SET first_name = NULL where first_name = ''""")
    c.execute("""UPDATE dedupe.entries SET last_name = NULL where last_name=''""")
    c.execute("""UPDATE dedupe.entries SET ssn = NULL where ssn=''""")
    c.execute("""UPDATE dedupe.entries SET sex = NULL where sex=''""")
    c.execute("""UPDATE dedupe.entries SET race = NULL where race=''""")
    c.execute("""UPDATE dedupe.entries SET ethnicity = NULL where ethnicity=''""")
    con.commit()
    con.close()

if __name__ == '__main__':
    main()
