"""
unit tests for combine_county_databases
"""

import combine_county_databases
import unittest


class TestCountyDBPattern(unittest.TestCase):
    def test_matches(self):
        example = 'c37019y2014_nr_20151211'
        self.assertTrue(bool(
            combine_county_databases.COUNTY_PATTERN.match(example)))

    def test_doesnt_match(self):
        example = 'sys'
        self.assertFalse(bool(
            combine_county_databases.COUNTY_PATTERN.match(example)))

if __name__ == '__main__':
    unittest.main()
