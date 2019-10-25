from ..settings import storage_configs, local_configs, gcp_configs
from .base import BlobStorage
from .local import LocalStorage
from .gcp import GoogleCloudStorage


def BlobStorageFactory(provider="local") -> BlobStorage:
    """Create a storage provider.

    Args:
        provider: the name of the storage provider. Choose among 
            `local` and `gcp`.
    
    Return: a storage provider of the class `BlobStorage`.
    """
    if provider == "local":
        root = local_configs.get("root")
        return LocalStorage(root)
    if provider == "gcp":
        project_id = gcp_configs.get("project_id")
        bucket_name = gcp_configs.get("bucket_name")
        service_account_file = gcp_configs.get("service_account_file")
        return GoogleCloudStorage(project_id=project_id, 
                bucket_name=bucket_name,
                service_account_file=service_account_file)
    raise ValueError("Uknown provider: "+provider)


_provider = storage_configs.get("provider")
storage = BlobStorageFactory(provider=_provider)