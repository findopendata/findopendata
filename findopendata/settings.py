import os

import yaml


def from_yaml(filename):
    with open(filename, "r") as f:
        return yaml.load(f, Loader=yaml.SafeLoader)


# Configurations.
configs = from_yaml(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.path.pardir,
    "configs.yaml"
))

# Storage configurations.
storage_configs = configs.get("storage")

# Local storage configurations.
local_configs = configs.get("local")

# GCP configurations.
gcp_configs = configs.get("gcp")

# Azure configurations.
azure_configs = configs.get("azure")

# Crawler configurations.
crawler_configs = configs.get("crawler")

# Postgres configurations.
db_configs = configs.get("postgres")

# Celery configurations.
celery_configs = configs.get("celery")

# Index configurations.
index_configs = configs.get("index")
