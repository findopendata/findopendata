from os import environ
import warnings

from google.cloud import datastore


def _get_env(name):
    var = environ.get(name, None)
    if var is None:
        raise ValueError("{} is None".format(name))
    return var


try:
    client = datastore.Client()
    entities = list(client.query(kind="Settings").fetch(limit=1))

    def _get_datastore(name):
        if len(entities) == 0 or name not in entities[0]:
            return _get_env(name)
        return entities[0][name]

    get = _get_datastore

except Exception as e:
    warnings.warn("Unable to use Cloud Datastore due to error: {} "
            "Using environment variables for settings.".format(e))
    get = _get_env
