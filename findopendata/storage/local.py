import os
import contextlib
import shutil
import gzip

import simplejson as json
import fastavro

from .base import BlobStorage, Blob


class LocalStorage(BlobStorage):
    """Local storage provider that utilizes the local file system.

    Args:
        root: the root directory, will be created if not exists.
    """

    def __init__(self, root):
        self._root = root
        # Create if not exists.
        if not os.path.exists(self._root):
            os.makedirs(self._root)
    
    def _get_and_check_path(self, blob_name):
        path = os.path.join(self._root, blob_name)
        if not os.path.exists(path):
            raise ValueError("Cannot find blob at path: "+path)
        return path
    
    def _get_path_and_create_dir(self, blob_name):
        path = os.path.join(self._root, blob_name)
        path_dir = os.path.dirname(path)
        if not os.path.exists(path_dir):
            os.makedirs(path_dir)
        return path
    
    def get_object(self, blob_name):
        path = self._get_and_check_path(blob_name)
        with open(path, "r") as f:
            obj = json.load(f)
        return obj

    @contextlib.contextmanager
    def get_file(self, blob_name):
        path = self._get_and_check_path(blob_name)
        try:
            fileobj = open(path, 'rb')
            yield fileobj
        finally:
            fileobj.close()
    
    def put_file(self, fileobj, blob_name):
        path = self._get_path_and_create_dir(blob_name)
        with open(path, 'wb') as f:
            shutil.copyfileobj(fileobj, f)
        size = os.path.getsize(path)
        return Blob(blob_name, size)
    
    def put_object(self, obj, blob_name):
        path = self._get_path_and_create_dir(blob_name)
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
    
    def put_json(self, records, blob_name, gzip_compress=True):
        path = self._get_path_and_create_dir(blob_name)
        newline = "\n"
        if gzip_compress:
            with gzip.open(path, "wt") as f:
                for record in records:
                    f.write(json.dumps(record))
                    f.write(newline)
        else:
            with open(path, "w") as f:
                for record in records:
                    f.write(json.dumps(record))
                    f.write(newline)
        size = os.path.getsize(path)
        return Blob(blob_name, size)
