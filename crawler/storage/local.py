import gzip
import os
import contextlib
import shutil

import simplejson as json
import fastavro

from .base import BlobStorage, Blob


class LocalStorage(BlobStorage):
    """Local storage provider that utilizes the local file system.

    Args:
        root: the root directory, will be created if not exists.
    """

    def __init__(self, root):
        self.root = root
        # Create if not exists.
        if not os.path.exists(self.root):
            os.makedirs(self.root)
    
    def _get_and_check_path(self, blob_name):
        path = os.path.join(self.root, blob_name)
        if not os.path.exists(path):
            raise ValueError("Cannot find blob at path: "+path)
        return path
    
    def _get_path_and_create_dir(self, blob_name):
        path = os.path.join(self.root, blob_name)
        path_dir = os.path.dirname(path)
        if not os.path.exists(path_dir):
            os.makedirs(path_dir)
        return path
    
    def get_object(self, blob_name, gzip_decompress=False):
        path = self._get_and_check_path(blob_name)
        if gzip_decompress:
            with gzip.open(path, 'rt') as f:
                obj = json.load(f)
        else:
            with open(path, "r") as f:
                obj = json.load(f)
        return obj

    @contextlib.contextmanager
    def get_file(self, blob_name, gzip_decompress=False):
        path = self._get_and_check_path(blob_name)
        try:
            if gzip_decompress:
                fileobj = gzip.open(path, 'rb')
            else:
                fileobj = open(path, 'rb')
            yield fileobj
        finally:
            fileobj.close()
    
    def put_file(self, fileobj, blob_name, gzip_compress=False):
        path = self._get_path_and_create_dir(blob_name)
        if gzip_compress:
            with gzip.open(path, 'wb') as f:
                shutil.copyfileobj(fileobj, f)
        else:
            with open(path, 'wb') as f:
                shutil.copyfileobj(fileobj, f)
        size = os.path.getsize(path)
        return Blob(blob_name, size)
    
    def put_object(self, obj, blob_name, gzip_compress=False):
        path = self._get_path_and_create_dir(blob_name)
        if gzip_compress:
            with gzip.open(path, 'wt') as f:
                json.dump(obj, f)
        else:
            with open(path, "w") as f:
                json.dump(obj, f)
        size = os.path.getsize(path)
        return Blob(blob_name, size)
    
    def put_avro(self, schema, records, blob_name, codec='snappy'):
        path = self._get_path_and_create_dir(blob_name)
        with open(path, "wb") as f:
            fastavro.writer(f, schema, records, codec)
        size = os.path.getsize(path)
        return Blob(blob_name, size)
