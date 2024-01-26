# Bulletin template tags
Forecast bulletins are written in the xlsx format. The bulletins are written to the folder data/bulletins. The templates can contain several sheets but only the first sheet of the bulletin template is used by the forecast tools to write to. The same logic is used for the bulletin template as in the iEasyHydro software. Terms in the template bulletin indicated by {{}} are replaced by values by the linear regression tool. Please see the list below for a list of available tags and use the available templates as reference.

Two bulletin formats are currently implemented:
- The regular pentadal forecast bulletin
- The traditional pentadal forecast sheet

Note that the traditional forecast sheet is only written when the option is selected in the configuration dashboard (tick box Записать Эксель файл).

## Tags available for forecast bulletin templates

The following tags are currently implemented for the regular pentadal forecast bulletin:

PENTAD
Will write the pentad for which the pentadal forecast is produced.

MONTH_STR_CASE1
Will write the name of the forecast month in the first case.

MONTH_STR_CASE2
Will write the name of the forecast month in the second case.

YEAR
Writes the year of the forecast period.

DAY_START
Writes the first day of the forecast pentad.

DAY_END
Writs the last day of the forecast pentad.

HEADER.BASIN
This is a header tag for the basin under which forecasts for stations within a basin are grouped.

DATA.PUNKT_NAME
Writes the name of the gauge station.

DATA.RIVER_NAME
Writes the name of the river.

DATA.QMIN
Writes the lower range of the forecasted discharge.

DATA.QMAX
Writes the upper range of the forecast discharge.

DATA.QNORM
Writes the norm discharge.

DATA.PERC_NORM
Writes the percentage of the forecasted discharge from the norm discharge.

DATA.QDANGER
Writes the dangerous discharge level.

DATA.DASH
Writes the symbol -.



# Tags available for the templates for the traditional pentadal forecast sheets

The following tags are currently implemented for the traditional pentadal forecast sheets:

HEADER.FSHEETS_RIVER_NAME
Writes the name of the river.

MONTH_STR_CASE1
Writes the name of the month in first case.

PENTAD
Writes the pentad of the month for the current forecast.

DATA.YEARFSHEETS
Writes rows of years.

DATA.QPAVG
Writes rows of average discharge for the current pentad.

DATA.QPSUM
Writes rows of the sum discharge of the past 3 days.


