import os
import io
import unittest
import tempfile

from findopendata.storage.local import LocalStorage
from findopendata.parsers.avro import avro2json

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

class TestLocalStorage(unittest.TestCase):

    def test_put_and_get_object(self):
        with tempfile.TemporaryDirectory() as root:
            storage = LocalStorage(root)
            blob = storage.put_object(test_obj, "test_blob")
            self.assertEqual(blob.name, "test_blob")
            self.assertTrue(blob.size > 0)
            self.assertTrue(os.path.exists(os.path.join(root, "test_blob")))

            obj = storage.get_object("test_blob")
            self.assertEqual(test_obj, obj)
    
    def test_put_and_get_file(self):
        with tempfile.TemporaryDirectory() as root:
            storage = LocalStorage(root)
            fileobj = io.BytesIO(test_file_content.encode("utf-8"))
            blob = storage.put_file(fileobj, "test_blob")
            self.assertEqual(blob.name, "test_blob")
            self.assertGreater(blob.size, 0)
            self.assertTrue(os.path.exists(os.path.join(root, "test_blob")))

            with storage.get_file("test_blob") as f:
                data = f.read().decode("utf-8")
                self.assertEqual(data, test_file_content)
            
    def test_put_avro(self):
        with tempfile.TemporaryDirectory() as root:
            storage = LocalStorage(root)
            blob = storage.put_avro(test_avro_schema, test_avro_records, 
                    "test_blob")
            self.assertEqual(blob.name, "test_blob")
            self.assertGreater(blob.size, 0)
            self.assertTrue(os.path.exists(os.path.join(root, "test_blob")))

            with storage.get_file("test_blob") as f:
                records = avro2json(f)
                for r1, r2 in zip(records, test_avro_records):
                    self.assertEqual(dict(r1), r2)
            

if __name__ == "__main__":
    unittest.main()