import io
import unittest

import fastavro

from crawler.avro import JSON2AvroRecords


records = [
        {"username": "javasucks", "email": "go@example.com"},
        {"username": "moonshoot", "email": "moon@example.com"},
        {"username": "twilight", "email": "tw@example.com"},
        {"username": "birdeye", "email": "be@example.com", "amount": 0},
        {"username": "birdeye", "amount": 1000},
        ]


class TestJSON2AvroRecords(unittest.TestCase):

    def test_basic(self):
        test_records = JSON2AvroRecords((r for r in records))
        self.assertEqual(len(list(test_records.get())), 5)
        schema = test_records.schema
        self.assertEqual(len(schema["fields"]), 3)


if __name__ == "__main__":
    unittest.main()

