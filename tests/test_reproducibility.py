import tests.generate_fake_dataset as gen
import tests.initialize_db as initdb
import yaml
import testing.postgresql
import psycopg2
import psycopg2.extras

from unittest.mock import patch
from pgdedupe.utils import load_config, filename_friendly_hash, create_model_definition
from pgdedupe.run import process_options, preprocess, create_blocking, cluster, train


def test_reproducibility():
    """Test that two dedupers trained with the same config and data
    come up with the same results"""

    psql = testing.postgresql.Postgresql()
    with open('db.yaml', 'w') as f:
        yaml.dump(psql.dsn(), f)

    pop = gen.create_population(100)
    gen.create_csv(pop, 'pop.csv')

    initdb.init('db.yaml', 'pop.csv')

    dbconfig = load_config('db.yaml')

    base_config = {
        'schema': 'dedupe',
        'table': 'dedupe.entries',
        'key': 'entry_id',
        'fields': [
            {'field': 'ssn', 'type': 'String', 'has_missing': True},
            {'field': 'first_name', 'type': 'String'},
            {'field': 'last_name', 'type': 'String'},
            {'field': 'dob', 'type': 'String'},
            {'field': 'race', 'type': 'Categorical', 'categories': ['pacisland', 'amindian', 'asian', 'other', 'black', 'white']},
            {'field': 'ethnicity', 'type': 'Categorical', 'categories': ['hispanic', 'nonhispanic']},
            {'field': 'sex', 'type': 'Categorical', 'categories': ['M', 'F']}
        ],
        'interactions': [
            ['last_name', 'dob'],
            ['ssn', 'dob']
        ],
        'filter_condition': 'last_name is not null AND (ssn is not null OR (first_name is not null AND dob is not null))',
        'recall': 0.99,
        'prompt_for_labels': False,
        'seed': 0,
        'training_file': 'tests/dedup_postgres_training.json'
    }
    config = process_options(base_config)
    con = psycopg2.connect(cursor_factory=psycopg2.extras.RealDictCursor, **dbconfig)
    preprocess(con, config)

    # train two versions of the deduper with the same configuration
    with patch.dict('os.environ', {'PYTHONHASHSEED': '123'}):
        old_deduper = train(con, config)
    con = psycopg2.connect(cursor_factory=psycopg2.extras.RealDictCursor, **dbconfig)
    with patch.dict('os.environ', {'PYTHONHASHSEED': '123'}):
        new_deduper = train(con, config)

    # ensure that the two models come up with the same hash
    model_hash = filename_friendly_hash(create_model_definition(config, old_deduper))
    new_model_hash = filename_friendly_hash(create_model_definition(config, new_deduper))
    assert new_model_hash == model_hash

    # run clustering on each of the dedupers
    create_blocking(old_deduper, con, config)
    old_dupes = cluster(old_deduper, con, config)

    create_blocking(new_deduper, con, config)
    new_dupes = cluster(new_deduper, con, config)

    # each deduper should come up with the same list of clusters
    assert [records for records, scores in old_dupes] == [records for records, scores in new_dupes]
