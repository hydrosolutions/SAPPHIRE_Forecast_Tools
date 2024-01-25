import unittest
import datetime as dt
from iEasyHydroForecast import tag_library as tl
import pandas as pd


class TestTesting(unittest.TestCase):
    def test_method(self):
        self.assertTrue(tl.is_gregorian_date('2022-05-15'))
