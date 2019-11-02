import os
import io
import unittest
import tempfile

from azure.storage.blob import BlockBlobService

from crawler.storage.azure import AzureStorage
from crawler.parsers.avro import avro2json

test_obj = {
    "name" : "First name Last name",
    "email" : "example@email.com",
    "age" : 42,
    "accounts": [123, 432, 2123],
}

test_file_content = """
h1,h2,h3
a,b,c
e,f,g
1,2,3
"""

test_avro_records = [
    {"h1" : "a", "h2": "b", "h3": "c"},
    {"h1" : "d", "h2": "e", "h3": "f"},
    {"h1" : "1", "h2": "2", "h3": "3"},
]

test_avro_schema = {
    "name": "root",
    "type": "record",
    "fields": [
        {"name": "h1", "type": "string"}, 
        {"name": "h2", "type": "string"}, 
        {"name": "h3", "type": "string"}, 
    ],
}

azurite_connection_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
container_name = "findopendata-test"

class TestAzureStorage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.service = BlockBlobService(
                connection_string=azurite_connection_str)
        cls.service.create_container(container_name)
    
    @classmethod
    def tearDownClass(cls):
        cls.service.delete_container(container_name)

    def setUp(self):
        self.storage = AzureStorage(connection_string=azurite_connection_str,
                container_name=container_name)

    def test_put_and_get_object(self):
        blob = self.storage.put_object(test_obj, "test_object_blob")
        self.assertEqual(blob.name, "test_object_blob")
        self.__class__.service.exists(container_name, "test_object_blob")
        size = self.__class__.service.get_blob_properties(container_name, 
                "test_object_blob").properties.content_length
        self.assertEqual(blob.size, size)

        obj = self.storage.get_object("test_object_blob")
        self.assertEqual(obj, test_obj)
    
    def test_put_and_get_file(self):
        fileobj = io.BytesIO(test_file_content.encode("utf-8"))
        blob = self.storage.put_file(fileobj, "test_file_blob")
        self.assertEqual(blob.name, "test_file_blob")
        self.__class__.service.exists(container_name, "test_file_blob")
        size = self.__class__.service.get_blob_properties(container_name, 
                "test_file_blob").properties.content_length
        self.assertEqual(blob.size, size)

        with self.storage.get_file("test_file_blob") as f:
            data = f.read().decode("utf-8")
            self.assertEqual(data.strip(), test_file_content.strip())
            
    def test_put_and_get_lareg_file(self):
        test_large_file_content = test_file_content.encode("utf-8")*1000000
        fileobj = io.BytesIO(test_large_file_content)
        blob = self.storage.put_file(fileobj, "test_large_file_blob")
        self.assertEqual(blob.name, "test_large_file_blob")
        self.__class__.service.exists(container_name, "test_large_file_blob")
        size = self.__class__.service.get_blob_properties(container_name, 
                "test_large_file_blob").properties.content_length
        self.assertEqual(blob.size, size)

        with self.storage.get_file("test_large_file_blob") as f:
            data = f.read()
            self.assertEqual(data.strip(), test_large_file_content.strip())
            
    def test_put_avro(self):
        blob = self.storage.put_avro(test_avro_schema, test_avro_records, 
                "test_avro_blob")
        self.assertEqual(blob.name, "test_avro_blob")
        self.__class__.service.exists(container_name, "test_avro_blob")
        size = self.__class__.service.get_blob_properties(container_name, 
                "test_avro_blob").properties.content_length
        self.assertEqual(blob.size, size)

        with self.storage.get_file("test_avro_blob") as f:
            records = avro2json(f)
            for r1, r2 in zip(records, test_avro_records):
                self.assertEqual(dict(r1), r2)
            
    def test_put_avro_large(self):
        test_large_avro_records = test_avro_records*1000000
        blob = self.storage.put_avro(test_avro_schema, test_large_avro_records, 
                "test_avro_blob")
        self.assertEqual(blob.name, "test_avro_blob")
        self.__class__.service.exists(container_name, "test_avro_blob")
        size = self.__class__.service.get_blob_properties(container_name, 
                "test_avro_blob").properties.content_length
        self.assertEqual(blob.size, size)

        with self.storage.get_file("test_avro_blob") as f:
            records = avro2json(f)
            for r1, r2 in zip(records, test_large_avro_records):
                self.assertEqual(dict(r1), r2)
            

if __name__ == "__main__":
    unittest.main()