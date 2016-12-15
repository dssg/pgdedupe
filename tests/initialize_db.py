import yaml
import os
from os import path, system

# We'll shell out to `psql`, so set the environment variables for it:
with open("database.yml") as f:
    for k,v in yaml.load(f).items():
        os.environ['PG' + k.upper()] = v if v else ""
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
system("""psql -c "\copy dedupe.entries FROM 'test/people.csv' WITH CSV HEADER;" """)
system("""psql -c "alter table dedupe.entries add column entry_id SERIAL PRIMARY KEY;" """)
system("""psql -c "alter table dedupe.entries add column full_name VARCHAR;" """)
system("""psql -c "update dedupe.entries set full_name = first_name || ' ' || last_name;" """)
