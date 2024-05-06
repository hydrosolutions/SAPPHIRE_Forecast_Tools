# Pre-processing of operational runoff data for hydrological forecasting
This component allows reads daily river runoff data from excel files and, if access is available, from the iEasyHydro database. The script is intended to run at 11 o'clock every day. It therefore includes daily average discharge data from all dates prior to today and todays morning measurement of river runoff. The data is stored in a csv file.

## Input
- Configuration as described in doc/configuration.md
- Excel file(s) with daily river runoff data, one file per measurement site. The excel files have 2 header lines and one column for date in the format %d.%m.%Y and discharge as float in m3/s each. The first header line contains the unique code and name of the measurement site, separated by space.

## Output
- CSV file with daily river runoff data for each site. The file contains the columns 'code', 'date', and 'discharge' (in m3/s).




