from os import environ
import warnings

from google.cloud import datastore
import yaml


def from_datastore(kind):
    client = datastore.Client()
    entities = list(client.query(kind=kind).fetch(limit=1))
    if len(entities) == 0:
        raise ValueError("Cloud Datastore has no entities with kind {}".format(
            kind))
    return dict(entities[0])


def from_yaml(filename):
    with open(filename, "r") as f:
        return yaml.load(f, Loader=yaml.SafeLoader)
