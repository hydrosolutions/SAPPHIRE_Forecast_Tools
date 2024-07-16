# Models and scalers
If you want to use data from the SAPPHIRE data gateway, you will store your parameters for quantile mapping here. The data gateway extracts daily average air temperature and average daily precipitation sums for hydrological response units (HRUs) defined in the SAPPHIRE data gateway. We define each catchment by its outlet, typically a gauge station (hydropost), and combine several catchments into one shapefile (also refered to as HRU file) which is used to extract global weather data in the SAPPHIRE data gateway.

The HRU file format is as follows:

COLUMNS for the Parameters: 'code', 'a', 'b', 'wet_day'
where 'code' is the unique identifier of the hydropost, 'a', 'b' are the parameters for the quantile mapping with function y_downscaled = a * y_original^b and 'wet_day' is the threshold above which a day is considered wet and quantile mapping is applied (0 for temperature).

Saved as HRU{HRU_CODE}_T_params.csv and HRU{HRU_CODE}_P_params.csv. For example HRU12345_T_params.csv and HRU12345_P_params.csv.

