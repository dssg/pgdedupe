import datetime
import hashlib
import json
import yaml
import io
import os
import logging
import pickle


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
    training_examples = io.StringIO()
    deduper.writeTraining(training_examples)
    model_definition = {
        'seed': config['seed'],
        'pythonhashseed': os.environ.get('PYTHONHASHSEED'),
        'fields': config['fields'],
        'filter_condition': config['filter_condition'],
        'interactions': config['interactions'],
        'training_examples': training_examples.getvalue(),
        'recall': config['recall'],
    }
    training_examples.close()
    return model_definition


def deduper_pickle_hash(deduper):
    
    #pickle.dump(self.data_model, file_obj)
    #pickle.dump(self.classifier, file_obj)
    #pickle.dump(self.predicates, file_obj)
    #import pdb
    #pdb.set_trace()
    settings_bytes = io.BytesIO()
    deduper.writeSettings(settings_bytes)
    pickle_hash = hashlib.md5(settings_bytes.getvalue()).hexdigest()
    settings_bytes.close()
    logging.info('Pickle hash = %s', pickle_hash)
    return pickle_hash
