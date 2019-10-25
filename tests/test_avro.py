import io
import unittest
from collections import OrderedDict

import fastavro

from crawler.parsers.avro import JSON2AvroRecords, avro2json


records = [
        {"username": "javasucks", "email": "go@example.com"},
        {"username": "moonshoot", "email": "moon@example.com"},
        {"username": "twilight", "email": "tw@example.com"},
        {"username": "birdeye", "email": "be@example.com", "amount": 0},
        {"username": "birdeye", "amount": 1000},
        ]

field_names = ["email", "username", "amount"]


class TestJSON2AvroRecords(unittest.TestCase):

    def test_basic(self):
        test_records = JSON2AvroRecords((r for r in records))
        self.assertEqual(len(list(test_records.get())), 5)
        schema = test_records.schema
        self.assertEqual(len(schema["fields"]), 3)

    def test_field_order(self):
        test_records = JSON2AvroRecords((r for r in records),
                field_names=field_names)
        schema = test_records.schema
        self.assertEqual([f["name"] for f in schema["fields"]], field_names)


class TestAvro2JSONRecords(unittest.TestCase):

    def test_avro2json(self):
        test_records = JSON2AvroRecords((r for r in records),
                field_names=field_names)
        buf = io.BytesIO(b'')
        fastavro.writer(buf, test_records.schema, test_records.get())
        buf.seek(0)
        for record in avro2json(buf):
            self.assertTrue(isinstance(record, OrderedDict))
            self.assertEqual(list(record.keys()), field_names)


if __name__ == "__main__":
    unittest.main()

