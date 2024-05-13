import os
import pandas as pd

from preprocessing_runoff.src import src

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

    output = src.read_runoff_data_from_multiple_rivers_xlsx(filename).reset_index(drop=True)

    assert output.equals(expected_output)

def test_read_all_runoff_data_from_excel():
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

    os.environ['ieasyforecast_daily_discharge_path'] = 'preprocessing_runoff/test/test_files'

    output = src.read_all_runoff_data_from_excel().reset_index(drop=True)

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
    src.write_data_to_csv(runoff_data)

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
        'Code': ['A', 'A', 'A', 'B', 'B', 'B'],
        'Q_m3s': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    })

    # Apply the function
    result = src.filter_roughly_for_outliers(df, 'Code', 'Q_m3s')

    # Drop index
    result = result.reset_index(drop=True)

    # Check that the result is the same as the input
    pd.testing.assert_frame_equal(result, df)

def test_filter_roughly_for_outliers_with_outliers():
    # Create a DataFrame with an outlier
    df = pd.DataFrame({
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
                  1.06, 2.06, 100.0, 4.0, 5.0, 6.0,
                  1.07, 2.07, 3.07, 4.0, 5.0, 6.0,
                  1.08, 2.08, 3.08, 4.0, 5.0, 6.0,
                  1.09, 2.09, 3.09, 4.0, 5.0, 6.0,
                  1.10, 2.10, 3.10, 4.0, 5.0, 6.0,
                  1.11, 2.11, 3.11, 4.0, 5.0, 6.0]
    })

    # Apply the function
    result = src.filter_roughly_for_outliers(df, 'Category', 'Values', window_size=15)

    # Check that the outlier has been replaced with NaN
    # There should be exactly one NaN value in the DataFrame column Q_m3s
    #print(result[result['Values']==100.0])
    #print(result['Values'].isna().sum())
    assert result['Values'].isna().sum() == 1
