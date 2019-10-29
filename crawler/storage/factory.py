import logging

from ..settings import azure_configs
from ..settings import local_configs
from ..settings import gcp_configs
from .base import BlobStorage
from .gcp import GoogleCloudStorage
from .azure import AzureStorage
from .local import LocalStorage


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
    if provider == "azure":
        # Set logging level
        connection_string = azure_configs.get("connection_string") 
        container_name = azure_configs.get("container_name")
        log_level = int(azure_configs.get("log_leve"))
        logging.getLogger("azure.storage.common.storageclient")\
                .setLevel(log_level)
        return AzureStorage(connection_string=connection_string,
                container_name=container_name)
    raise ValueError("Uknown provider: "+provider)
