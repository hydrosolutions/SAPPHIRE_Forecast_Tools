import unittest
import datetime as dt
from iEasyHydroForecast import tag_library as tl
import pandas as pd

class TestTagLibrary(unittest.TestCase):
    def test_is_gregorian_date_valid_date(self):
        self.assertTrue(tl.is_gregorian_date('2022-05-15'))
        self.assertTrue(tl.is_gregorian_date('1583-10-04'))
        self.assertTrue(tl.is_gregorian_date('1583-10-15'))
        self.assertTrue(tl.is_gregorian_date('1583-12-31'))
        self.assertTrue(tl.is_gregorian_date(dt.datetime(2022, 5, 15)))

    def test_is_gregorian_date_invalid_date(self):
        self.assertFalse(tl.is_gregorian_date('not a date'))
        self.assertFalse(tl.is_gregorian_date('2022-02-29'))
        self.assertFalse(tl.is_gregorian_date('1581-12-31'))

class TestAddPentadInYearColumn(unittest.TestCase):
    def test_add_pentad_in_year_column(self):
        # Create a test DataFrame
        df = pd.DataFrame({'Date': ['2022-05-15', '2022-05-16']})

        # Call the method to add the 'pentad' column
        df_with_pentad = tl.add_pentad_in_year_column(df)

        # Check that the 'pentad' column was added correctly
        self.assertEqual(df_with_pentad['pentad'].tolist(), ['27', '28'])

    def test_add_pentad_in_year_column_with_missing_date_column(self):
        # Create a test DataFrame without a 'Date' column
        df = pd.DataFrame({'Value': [1, 2]})

        # Call the method and check that it raises a ValueError
        with self.assertRaises(ValueError):
            tl.add_pentad_in_year_column(df)

    def test_add_pentad_in_year_column_with_invalid_date(self):
        # Create a test DataFrame with an invalid date
        df = pd.DataFrame({'Date': ['2022-05-15', 'invalid date']})

        # Call the method and check that it raises a ValueError
        with self.assertRaises(ValueError):
            tl.add_pentad_in_year_column(df)

class TestGetPentadInYear(unittest.TestCase):
    def test_valid_date(self):
        # Test a valid date
        result = tl.get_pentad_in_year('2022-05-15')
        self.assertEqual(result, '27')
        self.assertEqual(tl.get_pentad_in_year('2022-01-01'), '1')
        self.assertEqual(tl.get_pentad_in_year('2022-01-06'), '2')
        self.assertEqual(tl.get_pentad_in_year('2022-01-30'), '6')
        self.assertEqual(tl.get_pentad_in_year('2022-02-02'), '7')
        self.assertEqual(tl.get_pentad_in_year('2022-12-29'), '72')
        self.assertEqual(tl.get_pentad_in_year(dt.datetime(2022,12,31)), '72')

    def test_invalid_date(self):
        # Test an invalid date
        result = tl.get_pentad_in_year('not a date')
        self.assertIsNone(result)

    def test_non_gregorian_date(self):
        # Test a non-Gregorian date
        result = tl.get_pentad_in_year('1581-12-31')
        self.assertIsNone(result)

    def test_datetime_input(self):
        # Test a datetime input
        date = dt.datetime(2022, 5, 15)
        result = tl.get_pentad_in_year(date)
        self.assertEqual(result, '27')

    def test_invalid_input(self):
        # Test an invalid input
        result = tl.get_pentad_in_year(12345)
        self.assertIsNone(result)


class TestGetPentad(unittest.TestCase):

    def test_valid_date(self):
        # Test with a valid date
        date = dt.date(2022, 1, 1)
        pentad = tl.get_pentad_in_year(date)
        self.assertEqual(pentad, '1')
        self.assertEqual(tl.get_pentad_in_year('2022-12-29'), '36')

    def test_invalid_date(self):
        # Test with an invalid date
        date = '2022-13-01'
        pentad = tl.get_pentad_in_year(date)
        self.assertIsNone(pentad)

    def test_non_gregorian_date(self):
        # Test with a non-Gregorian date
        date = dt.date(1222, 1, 14)
        pentad = tl.get_pentad_in_year(date)
        self.assertIsNone(pentad)

    def test_string_input(self):
        # Test with a string input
        date = '2022-01-01'
        pentad = tl.get_pentad_in_year(date)
        self.assertEqual(pentad, '1')

    def test_valid_date(self):
        self.assertEqual(tl.get_pentad(dt.datetime(2022,5,15)), '3')
        self.assertEqual(tl.get_pentad('2022-05-15'), '3')
        self.assertEqual(tl.get_pentad('2022-05-01'), '1')
        self.assertEqual(tl.get_pentad('2022-05-31'), '6')
        self.assertEqual(tl.get_pentad('2022-02-28'), '6')
        self.assertEqual(tl.get_pentad('2022-02-01'), '1')
        self.assertEqual(tl.get_pentad('2022-02-05'), '1')
        self.assertEqual(tl.get_pentad('2022-02-06'), '2')
        self.assertEqual(tl.get_pentad('2022-02-10'), '2')
        self.assertEqual(tl.get_pentad('2022-02-11'), '3')
        self.assertEqual(tl.get_pentad('2022-02-15'), '3')
        self.assertEqual(tl.get_pentad('2022-02-16'), '4')
        self.assertEqual(tl.get_pentad('2022-02-20'), '4')
        self.assertEqual(tl.get_pentad('2022-02-21'), '5')
        self.assertEqual(tl.get_pentad('2022-02-25'), '5')
        self.assertEqual(tl.get_pentad('2022-02-26'), '6')

    def test_invalid_date(self):
        self.assertIsNone(tl.get_pentad('2022-02-29'))
        self.assertIsNone(tl.get_pentad('2022-13-01'))
        self.assertIsNone(tl.get_pentad('2022-00-01'))
        self.assertIsNone(tl.get_pentad('2022-01-00'))
        self.assertIsNone(tl.get_pentad('2022-01-32'))
        self.assertIsNone(tl.get_pentad('1581-10-04'))
        self.assertIsNone(tl.get_pentad('1581-10-15'))
        self.assertIsNone(tl.get_pentad('2100-01-01'))
        self.assertIsNone(tl.get_pentad('not a date'))


class TestGetStuffForBulletinWriting(unittest.TestCase):

    def test_get_pentad_first_day(self):
        self.assertEqual(tl.get_pentad_first_day('2022-05-15'), '11')
        self.assertEqual(tl.get_pentad_first_day('2022-05-01'), '1')
        self.assertEqual(tl.get_pentad_first_day('2022-05-31'), '26')
        self.assertEqual(tl.get_pentad_first_day('2022-02-28'), '26')
        self.assertIsNone(tl.get_pentad_first_day('2022-02-29'))

    def test_get_pentad_last_day(self):
        self.assertEqual(tl.get_pentad_last_day('2022-05-15'), '15')
        self.assertEqual(tl.get_pentad_last_day('2022-01-15'), '15')
        self.assertEqual(tl.get_pentad_last_day('2022-05-01'), '5')
        self.assertEqual(tl.get_pentad_last_day('2022-05-31'), '31')
        self.assertEqual(tl.get_pentad_last_day('2022-05-16'), '20')
        self.assertEqual(tl.get_pentad_last_day('2022-02-14'), '15')
        self.assertIsNone(tl.get_pentad_last_day('2022-02-30'))

    def test_get_year(self):
        self.assertEqual(tl.get_year('2022-05-15'), '2022')
        self.assertEqual(tl.get_year('2021-12-31'), '2021')
        self.assertIsNone(tl.get_year('2022-02-30'))

    def test_get_month_str_case1(self):
        self.assertEqual(tl.get_month_str_case1('2022-01-15'), 'январь')
        self.assertEqual(tl.get_month_str_case1('2022-02-28'), 'февраль')
        self.assertEqual(tl.get_month_str_case1('2022-03-31'), 'март')
        self.assertEqual(tl.get_month_str_case1('2022-04-15'), 'апрель')
        self.assertEqual(tl.get_month_str_case1('2022-05-01'), 'май')
        self.assertEqual(tl.get_month_str_case1('2022-06-15'), 'июнь')
        self.assertEqual(tl.get_month_str_case1('2022-07-31'), 'июль')
        self.assertEqual(tl.get_month_str_case1('2022-08-15'), 'август')
        self.assertEqual(tl.get_month_str_case1('2022-09-30'), 'сентябрь')
        self.assertEqual(tl.get_month_str_case1('2022-10-15'), 'октябрь')
        self.assertEqual(tl.get_month_str_case1('2022-11-30'), 'ноябрь')
        self.assertEqual(tl.get_month_str_case1('2022-12-15'), 'декабрь')
        self.assertIsNone(tl.get_month_str_case1('2022-02-30'))

    def test_get_month_str_case2(self):
        self.assertEqual(tl.get_month_str_case2('2022-01-15'), 'января')
        self.assertEqual(tl.get_month_str_case2('2022-02-28'), 'февраля')
        self.assertEqual(tl.get_month_str_case2('2022-03-31'), 'марта')
        self.assertEqual(tl.get_month_str_case2('2022-04-15'), 'апреля')
        self.assertEqual(tl.get_month_str_case2('2022-05-01'), 'мая')
        self.assertEqual(tl.get_month_str_case2('2022-06-15'), 'июня')
        self.assertEqual(tl.get_month_str_case2('2022-07-31'), 'июля')
        self.assertEqual(tl.get_month_str_case2('2022-08-15'), 'августа')
        self.assertEqual(tl.get_month_str_case2('2022-09-30'), 'сентября')
        self.assertEqual(tl.get_month_str_case2('2022-10-15'), 'октября')
        self.assertEqual(tl.get_month_str_case2('2022-11-30'), 'ноября')
        self.assertEqual(tl.get_month_str_case2('2022-12-15'), 'декабря')
        self.assertIsNone(tl.get_month_str_case2('2022-02-30'))


if __name__ == '__main__':
    unittest.main()