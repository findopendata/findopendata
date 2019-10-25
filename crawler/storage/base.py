import abc
import contextlib


class Blob(object):

    def __init__(self, name: str, size: int):
        self._name = name
        self._size = size
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def size(self) -> int:
        return self._size


class BlobStorage(object):

    @abc.abstractmethod
    def get_object(self, blob_name, gzip_decompress=False):
        """Get a JSON blob as a Python dictionary object.

        Args:
            blob_name: the name of the blob.
            gzip_decompress (default False): whether to gzip decompress 
                the blob.

        Returns: a Python dictionary.
        """
        pass

    @abc.abstractmethod
    @contextlib.contextmanager
    def get_file(self, blob_name, gzip_decompress=False):
        """Get a blob as an opened binary file for readonly.

        Args:
            blob_name: the name of the blob.
            gzip_decompress (default False): whether to gzip decompress 
                the blob.
        
        Returns: a binary file object.

        Example:

            with storage.get_file(blob_name) as f:
                # Do stuff
        """
        pass

    @abc.abstractmethod
    def put_file(self, fileobj, blob_name, gzip_compress=False):
        """Save a flat file to the storage.

        Args:
            fileobj: the file object to be uploaded to the bucket.
            blob_name: the name of the destination blob.
            gzip_compress (default False): whether to gzip compress the 
                input fileobj.

        Returns:
            blob: the blob object that was created.
        """
        pass

    @abc.abstractmethod
    def put_object(self, obj, blob_name, gzip_compress=False):
        """Save a single JSON-serializable Python dictionary to storage.

        Args:
            obj: the Python dictionary to be JSON-serialized and saved to 
                storage.
            blob_name: the name of the destination blob.
            gzip_compress (default False): whether to gzip compress the 
                blob.

        Returns:
            blob: the blob object that was created.
        """
        pass

    @abc.abstractmethod
    def put_avro(self, schema, records, blob_name, codec="snappy"):
        """Save Avro records to storage.

        Args:
            schema: the Avro schema to use.
            records: an iterator of records of type `dict`.
            blob_name: the name of the destination blob.
            codec: the compression codec to use (e.g., `snappy`).

        Returns:
            blob: the blob object that was created.
        """
        pass
