"""This module has the initialized storage object.
"""

from ..settings import storage_configs
from .factory import BlobStorageFactory

_provider = storage_configs.get("provider")
storage = BlobStorageFactory(provider=_provider)
