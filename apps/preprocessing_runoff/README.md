# Pre-processing of operational runoff data for hydrological forecasting
This component allows reads daily river runoff data from excel files and, if access is available, from the iEasyHydro database. The script is intended to run at 11 o'clock every day. It therefore includes daily average discharge data from all dates prior to today and todays morning measurement of river runoff. The data is stored in a csv file.

We perform a rough filtering of the data to remove outliers. The filtering is based on the assumption that the discharge should not change too much from one day to the next. We calculate the daily difference of the discharge and remove all values that are more than 3 times the standard deviation from the mean.

## Input
- Configuration as described in doc/configuration.md
- Excel file(s) with daily river runoff data, one file per measurement site. The excel files have 2 header lines and one column for date in the format %d.%m.%Y and discharge as float in m3/s each. The first header line contains the unique code and name of the measurement site, separated by space. Some discharge data may come in a different format, with one header line only and the code ID in the name of the file. In this case, the code is extracted from the file name.

## Output
- CSV file with daily river runoff data for each site. The file contains the columns 'code', 'date', and 'discharge' (in m3/s).




