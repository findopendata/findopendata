import unittest

from crawler.socrata import socrata_records

class TestSocrataRecords(unittest.TestCase):

    resource_url = "https://soda.demo.socrata.com/resource/b6kv-3wgw.json"

    def test_demo_api(self):
        records = socrata_records(self.resource_url, app_token=None)
        self.assertTrue(all(isinstance(record, dict) for record in records))


if __name__ == "__main__":
    unittest.main()

