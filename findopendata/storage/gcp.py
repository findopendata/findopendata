import os
import contextlib
import gzip

import simplejson as json
import fastavro
from google.oauth2 import service_account
from google.cloud import storage
from gcsfs.core import GCSFileSystem

from .base import BlobStorage, Blob


class GoogleCloudStorage(BlobStorage):
    """Google Cloud Storage storage provider.

    Args:
        project_id: the ID of the Google Cloud project.
        bucket_name: the name of the Cloud Storage bucket to use for all blobs.
        service_account_file: the filename of the GCP service account JSON key 
            file.
    """
    def __init__(self, project_id: str, bucket_name: str, 
            service_account_file: str):
        self._bucket_name = bucket_name
        self._client = storage.Client(project=project_id, 
                credentials=service_account.Credentials.\
                from_service_account_file(service_account_file))
        self._fs = GCSFileSystem(token=service_account_file,
                check_connection=True)
    
    def get_object(self, blob_name):
        blob = self._client.bucket(self._bucket_name).get_blob(blob_name)
        if blob is None:
            raise ValueError("Cannot find blob: "+blob_name)
        return json.loads(blob.download_as_string().decode("utf-8"))

    @contextlib.contextmanager
    def get_file(self, blob_name):
        path = os.path.join(self._bucket_name, blob_name)
        try:
            fileobj = self._fs.open(path, 'rb')
            yield fileobj
        finally:
            fileobj.close()

    def put_object(self, obj, blob_name):
        blob = self._client.bucket(self._bucket_name).blob(blob_name)
        data = json.dumps(obj).encode("utf-8")
        blob.upload_from_string(data, content_type="application/json")
        blob.reload()
        return Blob(blob_name, blob.size)

    def put_file(self, fileobj, blob_name):
        blob = self._client.bucket(self._bucket_name).blob(blob_name)
        blob.upload_from_file(fileobj)
        blob.reload()
        return Blob(blob_name, blob.size)
    
    def put_avro(self, schema, records, blob_name, codec='snappy'):
        path = os.path.join(self._bucket_name, blob_name)
        tmp_path = os.path.join(os.path.dirname(path),
                "~{}".format(os.path.basename(path)))
        with self._fs.open(tmp_path, "wb") as of:
            fastavro.writer(of, schema, records, codec)
        self._fs.mv(tmp_path, path)
        self._fs.setxattrs(path, content_type="avro/binary")
        blob = self._client.bucket(self._bucket_name).get_blob(blob_name)
        if blob is None:
            raise RuntimeError("Cannot find new avro blob: "+blob_name)
        return Blob(blob_name, blob.size)
    
    def put_json(self, records, blob_name, gzip_compress=True):
        path = os.path.join(self._bucket_name, blob_name)
        tmp_path = os.path.join(os.path.dirname(path),
                "~{}".format(os.path.basename(path)))
        newline = "\n"
        with self._fs.open(tmp_path, "wb") as of:
            if gzip_compress:
                with gzip.open(of, "wt") as of:
                    for record in records:
                        of.write(json.dumps(record))
                        of.write(newline)
            else:
                for record in records:
                    of.write(json.dumps(record))
                    of.write(newline)
        self._fs.mv(tmp_path, path)
        self._fs.setxattrs(path, content_type="application/json")
        blob = self._client.bucket(self._bucket_name).get_blob(blob_name)
        if blob is None:
            raise RuntimeError("Cannot find new JSON blob: "+blob_name)
        return Blob(blob_name, blob.size)
