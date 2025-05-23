import os
import pandas as pd
import datetime as dt
import pytest

from preprocessing_runoff.src import src

def test_get_runoff_data_no_data_available():

    os.environ['ieasyforecast_daily_discharge_path'] = 'preprocessing_runoff/test/test_files/test_config'

    output = src.get_runoff_data()
    print("Output: ")
    print(output)

    os.environ.pop('ieasyforecast_daily_discharge_path')

def test_read_runoff_data_from_multiple_rivers_xlsx():
    filename = 'preprocessing_runoff/test/test_files/test_runoff_file.xlsx'
    expected_output = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7,
                      4.3, 4.4, 4.5, 4.6, 4.7],
        'name': ['s. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch',
                  'other r. - hi', 'other r. - hi', 'other r. - hi',
                  'other r. - hi', 'other r. - hi'],
        'code': [17123, 17123, 17123, 17123, 17123,
                 17456, 17456, 17456, 17456, 17456]
    }).reset_index(drop=True)
    expected_output['date'] = pd.to_datetime(expected_output['date']).dt.date

    output = src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=['17123', '17456']).reset_index(drop=True)

    assert output.equals(expected_output)

def test_read_runoff_data_from_multiple_rivers_no_code():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/test_runoff_file_no_code.xlsx'

    expected_output = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7],
        'name': ['s. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch', 's. n. wi - spec ch',
                  's. n. wi - spec ch'],
        'code': [17123, 17123, 17123, 17123, 17123]
    }).reset_index(drop=True)
    expected_output['date'] = pd.to_datetime(expected_output['date']).dt.date

    output = src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=['17123']).reset_index(drop=True)

    # assert if all values in column discharge are NaN
    assert output.equals(expected_output)

def test_read_runoff_data_from_multiple_rivers_without_data_in_xls():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/test_runoff_file_no_data.xlsx'

    output = src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=['17123']).reset_index(drop=True)

    # assert if all values in column discharge are NaN
    assert output['discharge'].isna().all()

def test_read_runoff_data_from_multiple_rivers_no_file():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/this_file_does_not_exist.xlsx'

    # Assert FileNotFoundError is raised
    with pytest.raises(FileNotFoundError):
        src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=[123])

def test_read_runoff_data_from_multiple_rivers_no_station_header():

    filename = 'preprocessing_runoff/test/test_files/files_with_errors/test_runoff_file_no_station_header.xlsx'

    with pytest.raises(ValueError):
        src.read_runoff_data_from_multiple_rivers_xlsx(filename, code_list=[123])

def test_read_all_runoff_data_from_excel():
    expected_output = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05',
                 '2001-01-01', '2001-01-02', '2001-01-03', '2001-01-04', '2001-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7,
                      4.3, 4.4, 4.5, 4.6, 4.7,
                      2.3, 2.4, 2.5, 2.6, 2.7,
                      4.3, 4.4, 4.5, 4.6, 4.7],
        'name': ['s. n. wi - spec ch', 's. n. wi - spec ch',
                    's. n. wi - spec ch', 's. n. wi - spec ch',
                    's. n. wi - spec ch',
                    'other r. - hi', 'other r. - hi', 'other r. - hi',
                    'other r. - hi', 'other r. - hi',
                    '', '', '', '', '', '', '', '', '', ''],
        'code': [17123, 17123, 17123, 17123, 17123,
                 17456, 17456, 17456, 17456, 17456,
                 12345, 12345, 12345, 12345, 12345,
                 12345, 12345, 12345, 12345, 12345]
    }).reset_index(drop=True)
    expected_output['date'] = pd.to_datetime(expected_output['date']).dt.date

    os.environ['ieasyforecast_daily_discharge_path'] = 'preprocessing_runoff/test/test_files'

    output = src.read_all_runoff_data_from_excel(code_list=['17123', '17456', '12345']).reset_index(drop=True)

    os.environ.pop('ieasyforecast_daily_discharge_path')

    assert output.equals(expected_output)


def test_write_data_to_csv():
    runoff_data = pd.DataFrame({
        'date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05'],
        'discharge': [2.3, 2.4, 2.5, 2.6, 2.7],
        'name': ['a', 'a', 'a', 'a', 'a'],
        'code': [1, 1, 1, 1, 1]})

    # Define environment variables
    os.environ['ieasyforecast_intermediate_data_path'] = 'preprocessing_runoff/test/test_files'
    os.environ['ieasyforecast_daily_discharge_file'] = 'test_runoff_file.csv'

    # Write the output file
    src.write_daily_time_series_data_to_csv(runoff_data)

    # Read the output file
    output = pd.read_csv('preprocessing_runoff/test/test_files/test_runoff_file.csv')

    # The data in columns date, discharge and code should be the same
    assert output['date'].equals(runoff_data['date'])
    assert output['discharge'].equals(runoff_data['discharge'])
    assert output['code'].equals(runoff_data['code'])

    # Clean up the environment variables
    os.environ.pop('ieasyforecast_intermediate_data_path')
    os.environ.pop('ieasyforecast_daily_discharge_file')

    # Remove the output file
    os.remove('preprocessing_runoff/test/test_files/test_runoff_file.csv')


def test_filter_roughly_for_outliers_no_outliers():
    # Create a DataFrame with no outliers
    df = pd.DataFrame({
        'Date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-01', '2000-01-02', '2000-01-03'],
        'Code': ['A', 'A', 'A', 'B', 'B', 'B'],
        'Q_m3s': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    })
    # Convert df['Date'] to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Apply the function
    result = src.filter_roughly_for_outliers(df, 'Code', 'Q_m3s', 'Date')

    # Drop index
    result = result.reset_index(drop=True)

    # Check that the result is the same as the input
    pd.testing.assert_frame_equal(result, df, check_like=True)

def test_filter_roughly_for_outliers_with_outliers():
    # Create a DataFrame with an outlier
    df = pd.DataFrame({
        'Date': ['2000-01-01', '2000-01-02', '2000-01-03', '2000-01-01', '2000-01-02', '2000-01-03',
                 '2000-01-04', '2000-01-05', '2000-01-06', '2000-01-04', '2000-01-05', '2000-01-06',
                 '2000-01-07', '2000-01-08', '2000-01-09', '2000-01-07', '2000-01-08', '2000-01-09',
                 '2000-01-10', '2000-01-11', '2000-01-12', '2000-01-10', '2000-01-11', '2000-01-12',
                 '2000-01-13', '2000-01-14', '2000-01-15', '2000-01-13', '2000-01-14', '2000-01-15',
                 '2000-01-16', '2000-01-17', '2000-01-18', '2000-01-16', '2000-01-17', '2000-01-18',
                 '2000-01-19', '2000-01-20', '2000-01-21', '2000-01-19', '2000-01-20', '2000-01-21',
                 '2000-01-22', '2000-01-23', '2000-01-24', '2000-01-22', '2000-01-23', '2000-01-24',
                 '2000-01-25', '2000-01-26', '2000-01-27', '2000-01-25', '2000-01-26', '2000-01-27',
                 '2000-01-28', '2000-01-29', '2000-01-30', '2000-01-28', '2000-01-29', '2000-01-30',
                 '2000-01-31', '2000-02-01', '2000-02-02', '2000-01-31', '2000-02-01', '2000-02-02'],
        'Category': ['A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B',
                 'A', 'A', 'A', 'B', 'B', 'B'],
        'Values': [1.01, 2.01, 3.01, 4.0, 5.0, 6.0,
                  1.02, 2.02, 3.02, 4.0, 5.0, 6.0,
                  1.03, 2.03, 3.03, 4.0, 5.0, 6.0,
                  1.04, 2.04, 3.04, 4.0, 5.0, 6.0,
                  1.05, 2.05, 3.05, 4.0, 5.0, 6.0,
                  1.06, 2.06, 18.0, 4.0, 5.0, 6.0,
                  1.07, 2.07, 3.07, 4.0, 5.0, 6.0,
                  1.08, 2.08, 3.08, 4.0, 5.0, 6.0,
                  1.09, 2.09, 3.09, 4.0, 5.0, 6.0,
                  1.10, 2.10, 3.10, 4.0, 5.0, 6.0,
                  1.11, 2.11, 3.11, 4.0, 5.0, 6.0]
    })
    # Convert df['Date'] to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Apply the function
    result = src.filter_roughly_for_outliers(df, 'Category', 'Values', 'Date')
    # Print value on January 18th for category A
    new = (result[(result['Category']=='A') & (result['Date']=='2000-01-18')])
    old = (df[(df['Category']=='A') & (df['Date']=='2000-01-18')])

    # Check that the outlier has been replaced with NaN
    # There should be exactly one NaN value in the DataFrame column Q_m3s
    #print(result[result['Values']==100.0])
    #print(result['Values'].isna().sum())
    assert result['Values'].isna().sum() == 0
    # Assert that the outlier has been replaced with the linear interpolation
    assert new['Values'].values[0] != old['Values'].values[0]


class TestFromDailyTimeSeriestoHydrograph:
    """Test class for the from_daily_time_series_to_hydrograph function."""
    
    def test_leap_year_handling(self):
        """Test proper handling of leap years in hydrograph generation."""
        # Create a DataFrame spanning multiple years, including a leap year
        dates = []
        values = []
        
        # Create test data with dates from 2019-2021 (2020 is a leap year)
        for year in [2019, 2020, 2021]:
            # Create full year of data
            year_dates = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-31')
            dates.extend(year_dates)
            
            # Add some test values (just using day of year as the value)
            values.extend([date.dayofyear for date in year_dates])
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'discharge': values,
            'code': '15194',
            'name': 'Test Site'
        })
        
        # Run the function
        result = src.from_daily_time_series_to_hydrograph(df)
        
        # Check for leap year handling
        # We should have day_of_year values 1-365 (no 366 even though 2020 is a leap year)
        assert set(result['day_of_year'].unique()) == set(range(1, 366))
    
        # The dates in the result should be in the current year
        current_year = dt.date.today().year
        assert all(d.year == current_year for d in result['date'])
    
    # Verify that date sequence is continuous (no gaps)
        sorted_result = result.sort_values('date')
        date_diffs = sorted_result['date'].diff().iloc[1:].dt.days
        assert date_diffs.max() == 1
        assert date_diffs.min() == 1
    
    def test_statistics_calculation(self):
        """Test that statistics are correctly calculated for historical data."""
        # Get current year for testing
        current_year = dt.date.today().year
        last_year = current_year - 1
        
        # Create 5 years of data for day 1-3 of January with known patterns
        dates = []
        values = []
        
        for year in range(current_year-4, current_year+1):
            for day in range(1, 4):
                dates.append(dt.datetime(year, 1, day))
                if year == current_year:
                    values.append(day * 10)  # Current year values: 10, 20, 30
                elif year == last_year:
                    values.append(day * 5)   # Last year values: 5, 10, 15
                else:
                    values.append(day)       # Earlier years values: 1, 2, 3
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'discharge': values,
            'code': '15194',
            'name': 'Test Site'
        })
        
        # Run the function
        result = src.from_daily_time_series_to_hydrograph(df)
        
        # Check statistics for each day
        for day in range(1, 4):
            day_result = result[result['date'].dt.day == day].iloc[0]
            
            # Check count is correct (5 years of data)
            assert day_result['count'] == 5
            
            # Check mean (3 early years with value=day, last year with 5*day, current year with 10*day)
            expected_mean = (day*3 + day*5 + day*10) / 5
            assert abs(day_result['mean'] - expected_mean) < 0.0001
            
            # Check percentiles
            assert day_result['min'] == day  # Minimum is just the day value
            assert day_result['max'] == day * 10  # Maximum is current year value
            
            # Check current and previous year values
            assert day_result[str(current_year)] == day * 10
            assert day_result[str(last_year)] == day * 5
    
    def test_multiple_sites(self):
        """Test processing of multiple sites within the same dataset."""
        # Create test data for two sites
        dates = pd.date_range(start='2021-01-01', periods=10, freq='D')
        
        data = []
        for site_code in ['15194', '15212']:
            for date in dates:
                # Different pattern for each site
                if site_code == '15194':
                    value = date.day
                else:
                    value = date.day * 2
                
                data.append({
                    'date': date,
                    'discharge': value,
                    'code': site_code,
                    'name': f'Test Site {site_code}'
                })
        
        df = pd.DataFrame(data)
        
        # Run the function
        result = src.from_daily_time_series_to_hydrograph(df)
        
        # Verify each site is processed separately
        site_groups = result.groupby('code')
        assert len(site_groups) == 2
        
        # Check each site has the correct data
        site1_data = site_groups.get_group('15194')
        site2_data = site_groups.get_group('15212')
        
        # Both sites should have same number of days
        assert len(site1_data) == len(dates)
        assert len(site2_data) == len(dates)
        
        # Check that means reflect the different patterns
        for day in range(1, 11):
            site1_day = site1_data[site1_data['date'].dt.day == day]
            site2_day = site2_data[site2_data['date'].dt.day == day]
            
            if not site1_day.empty and not site2_day.empty:
                assert site1_day['mean'].iloc[0] == day
                assert site2_day['mean'].iloc[0] == day * 2


