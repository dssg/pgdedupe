import datetime
import hashlib
import json
import logging
import yaml
import os


def load_config(filename):
    ext = os.path.splitext(filename)[1].lower()
    with open(filename) as f:
        if ext == '.json':
            return json.load(f)
        elif ext in ('.yaml', '.yml'):
            return yaml.load(f)
        else:
            raise Exception('unknown filetype %s' % ext)


def filename_friendly_hash(inputs):
    def dt_handler(x):
        if isinstance(x, datetime.datetime) or isinstance(x, datetime.date):
            return x.isoformat()
        raise TypeError("Unknown type")
    return hashlib.md5(
        json.dumps(inputs, default=dt_handler, sort_keys=True)
            .encode('utf-8')
    ).hexdigest()


def create_model_definition(config, deduper):
    model_definition = {
        'seed': config['seed'],
        'pythonhashseed': os.environ.get('PYTHONHASHSEED'),
        'classifier': config['classifier'],
        'hyperparameters': config['hyperparameters'],
        'fields': config['fields'],
        'filter_condition': config['filter_condition'],
        'interactions': config['interactions'],
        'training_examples': deduper.training_pairs,
        'recall': config['recall'],
    }
    logging.debug('Model definition = %s', model_definition)
    return model_definition
