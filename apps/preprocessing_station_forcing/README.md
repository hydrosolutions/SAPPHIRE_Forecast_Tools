# Pre-processing of operational 10-day weather station data for hydrological forecasting
This component allows the reading of decadal precipitatin and temperature data from excel files (if they are available) and, if access is available, from the iEasyHydro database. The script is intended to run at 11 o'clock every day. It therefore includes daily average discharge data from all dates prior to today and todays morning measurement of river runoff. The data is stored in a csv file.

The preprocessing station forcing tool uses the same requirements as the preprocessing_discharge tool.

Note that currently, regime data is not available. Also norm data is not yet available.

## Input
- Configuration as described in doc/configuration.md

## Output
- CSV file with decadal precipitation and temperature data for each site. The file contains the columns 'code', 'date', and 'discharge' (in m3/s).




