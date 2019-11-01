import os
import json
import unittest
import tempfile

from crawler.socrata import socrata_records
from crawler.parsers.avro import JSON2AvroRecords
from crawler.storage.local import LocalStorage

class TestSocrataRecords(unittest.TestCase):

    resource_url = "https://soda.demo.socrata.com/resource/b6kv-3wgw.json"

    def test_demo_api(self):
        records = socrata_records(self.resource_url, app_token=None)
        self.assertTrue(all(isinstance(record, dict) for record in records))
    
    def test_save_avro_records(self):
        records = JSON2AvroRecords(socrata_records(self.resource_url, 
                app_token=None))
        with tempfile.TemporaryDirectory() as root:
            storage = LocalStorage(root)
            json.dump(records.schema, open("schema.json", "w"))
            blob = storage.put_avro(records.schema, records.get(), "test_blob")
            self.assertEqual(blob.name, "test_blob")
            self.assertGreater(blob.size, 0)
            self.assertTrue(os.path.exists(os.path.join(root, "test_blob")))


if __name__ == "__main__":
    unittest.main()

