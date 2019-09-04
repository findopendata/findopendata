import os

import yaml


def from_yaml(filename):
    with open(filename, "r") as f:
        return yaml.load(f, Loader=yaml.BaseLoader)


# Configurations.
configs = from_yaml(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.path.pardir,
    "configs.yaml"
))

# Crawler configurations.
crawler_configs = configs.get("crawler")

# GCP configurations.
gcp_configs = configs.get("gcp")

# Postgres configurations.
db_configs = configs.get("postgres")

# Celery configurations.
celery_configs = configs.get("celery")
