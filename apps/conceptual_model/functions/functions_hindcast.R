
# Summary: The function convert_daily_to_pentad_decad converts daily time steps to pentad or decad time steps by taking the mean of the median of the ensemble forecast over the needed time period form the daily forecast.
# Used in operational forecasting in the function process_save_time_steps. 
# Converts daily forecasts to pentadal and decadal.
# Input: forecast_statistics from the function calculate_stats_forecast.
# Output: forecast_statistics are always 15 days ahead. 
# For pentadal or decadal, only about 5 or 10 days ahead are needed. 
# The data takes only the necessary time length and calculates the mean over this period from the median of the ensemble forecast. 
# Output is a data frame with the forecast_date and the Qsim
convert_daily_to_pentad_decad <- function(forecast_data, time_steps, period) {
  if (period == "pentad") {
    forecast_data <- forecast_data %>%
      dplyr::filter(forecast_date %in% time_steps) %>%
      dplyr::mutate(period_value = get_pentad(date)) %>%
      dplyr::group_by(forecast_date) %>%
      dplyr::filter(period_value == min(period_value)) %>%
      dplyr::summarise(Qsim = mean(Q50))
    
    
  } else if (period == "decad") {
    forecast_data <- forecast_data %>%
      dplyr::filter(forecast_date %in% time_steps) %>%
      dplyr::mutate(period_value = get_decad(date)) %>%
      dplyr::group_by(forecast_date)  %>%
      dplyr::filter(period_value == min(period_value)) %>%
      dplyr::summarise(Qsim = mean(Q50))
  }
  return(forecast_data)
}



#' @title Generate Hindcast for a Specific Period
#'
#' @description
#' The `get_hindcast_period` function generates a hindcast over a specified time period for a given basin. 
#' It calls the `get_hindcast` function iteratively for each time step within the period defined by `start_date` and `end_date`. 
#' The `forecast_mode` parameter defines the frequency of the forecasts, which can be `daily`, `pentad`, or `decad`.
#'
#' @param start_date [Date] The start date of the hindcast period.
#' @param end_date [Date] The end date of the hindcast period.
#' @param forecast_mode [Character] The forecast mode: can be either `daily`, `pentad`, or `decad`.
#' @param lag_days [Numeric] Number of days after the initial two-year run to provide initial conditions without data assimilation, 
#' followed by the hydrological model running with data assimilation (default is 180 days).
#' @param Basin_Info [List] Information needed for the relative ice area and other basin-specific data.
#' @param basinObsTS [Data Frame] Observed time series data with the following columns: 
#' `date` [Date], `Temp` [Numeric] in Celsius, `Ptot` [Numeric] in mm/day, `Qmm` [Numeric] in mm/day, `PET` [Numeric] in mm/day.
#' @param FUN_MOD [Function] The function of the hydrological model, e.g., `RunModel_CemaNeigeGR4J_Glacier`, `RunModel_CemaNeigeGR6J`, etc.
#' @param parameter [Numeric] Parameter for the hydrological model.
#' @param NbMbr [Numeric] Number of ensemble members.
#' @param DaMethod [Character] Data assimilation method, e.g., `PF` for Particle Filter.
#' @param StatePert [Character] A vector of state variable names to be perturbed via Particle Filter: 
#' `"Prod"` - level of the production store [mm], 
#' `"Rout"` - level of the routing store [mm], 
#' `"UH1"` - unit hydrograph 1 levels [mm] (not defined for the GR5J model), 
#' `"UH2"` - unit hydrograph 2 levels [mm].
#' @param eps [Numeric] Perturbation factor for precipitation and potential evapotranspiration.
#'
#' @return [Data Frame] A data frame with hindcast results over the specified period. For daily there will be a column forecast_date and date (which start forecast date + 1 until 15 ahead). For pentad and decad there wil be 
get_hindcast_period <- function(start_date, 
                                end_date, 
                                forecast_mode, 
                                lag_days = 180, 
                                Basin_Info,
                                basinObsTS, 
                                FUN_MOD,
                                parameter, 
                                NbMbr, 
                                DaMethod, 
                                StatePert, 
                                eps) {
  
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  # print(start_date)
  # print(end_date)
  # print(forecast_mode)
  # check that start_date has to be smaller than end_date
  if (start_date > end_date) {
    stop("start_date has to be before the end_date")
  }
  
  
  if (forecast_mode == "pentad") {
    time_steps <- pentadal_days(start_date, end_date)
  } else if (forecast_mode == "daily") {
    time_steps <- seq(from = start_date, to = end_date, by = "days")
  } 
  
  inputsModel <- CreateInputsModel(FUN_MOD   = FUN_MOD,
                                          DatesR    = basinObsTS$date,
                                          Precip    = basinObsTS$Ptot, 
                                          PotEvap   = basinObsTS$PET,
                                          TempMean  = basinObsTS$Temp, 
                                          HypsoData = Basin_Info$HypsoData,
                                          ZInputs   = median(Basin_Info$HypsoData), 
                                          verbose = FALSE, 
                                          GradT = Basin_Info$GradT,
                                          GradP = Basin_Info$k_value)
  
  inputsPert <- CreateInputsPert(FUN_MOD = FUN_MOD,
                                 DatesR = basinObsTS$date,
                                 Precip = basinObsTS$Ptot,
                                 PotEvap = basinObsTS$PET,
                                 TempMean  = basinObsTS$Temp,
                                 HypsoData = Basin_Info$HypsoData,
                                 ZInputs   = median(Basin_Info$HypsoData),
                                 NbMbr = NbMbr,
                                 GradT = Basin_Info$GradT,
                                 GradP = Basin_Info$k_value,
                                 Eps_Ptot = eps, 
                                 Eps_PET = eps)
  
  
  # Loop over the time steps
  hindcast_all <- list()
  for (i in 1:length(time_steps)) {
    # get the hindcast
    forecast_date <- time_steps[i]
    print(forecast_date)
    hindcast <- get_hindcast(forecast_date = forecast_date,
                             forecast_mode = forecast_mode,
                             lag_days = lag_days,
                             Basin_Info = Basin_Info,
                             FUN_MOD = FUN_MOD,
                             parameter = parameter,
                             inputsModel = inputsModel,
                             inputsPert = inputsPert,
                             basinObsTS = basinObsTS,
                             DaMethod = DaMethod,
                             NbMbr = NbMbr,
                             StatePert = StatePert)
    
    # if the forecast_mode is pentadal
    if (forecast_mode == "pentad") {
      hindcast <- hindcast %>% 
        summarise(Qsim = mean(Q50)) %>%
        mutate(forecast_date = forecast_date) %>%
        dplyr::select(forecast_date, Qsim)
      
    } else if (forecast_mode == "daily") {
      hindcast <- hindcast %>%
        mutate(forecast_date = forecast_date) %>% 
        dplyr::select(forecast_date, everything())
    
      } else if (forecast_mode == "decad") {
      hindcast <- hindcast %>% 
        summarise(Qsim = mean(Q50)) %>%
        mutate(forecast_date = forecast_date) %>%
        dplyr::select(forecast_date, Qsim)
    }
    hindcast_all[[i]] <- hindcast
  }
  
  hindcast_all_df <- do.call(rbind, hindcast_all)
  return(hindcast_all_df)
}

#' @title Generate Hindcast for a Specific Date
#'
#' @description
#' The `get_hindcast` function generates a hindcast for a specific date and basin. 
#' It operates similarly to the operational workflow but exclusively uses ERA5-Land data 
#' as future data. The `lag_days` parameter determines how many days the hydrological model 
#' will run with data assimilation. To ensure optimal initial conditions before data 
#' assimilation begins, the model always runs for two years without data assimilation.
#' 
#' The `forecast_mode` can be one of the following: `daily`, `pentad`, or `decad`. 
#' In daily mode, the output forecasts extend 15 days ahead, meaning forecasts from the 
#' day after the forecast day up to 14 days into the future. For `pentad` and `decad`, 
#' forecasts align with the period length defined by the Kyrgyz hydromet.
#' 
#' This function can be combined with `get_hindcast_period` to generate a hindcast over 
#' a longer time period.
#'
#' @param forecast_date [Date] Date of the forecast; for `pentad` or `decad`, must be a date when a forecast is produced.
#' @param forecast_mode [Character] The forecast mode: can be either `daily`, `pentad`, or `decad`.
#' @param lag_days [Numeric] Number of days after the initial two-year run to provide initial conditions 
#' without data assimilation, followed by hydrological model running with data assimilation (default is 180 days).
#' @param Basin_Info [List] Information needed for the relative ice area.
#' @param FUN_MOD [Function] The function of the hydrological model, e.g., `RunModel_CemaNeigeGR4J_Glacier`, `RunModel_CemaNeigeGR6J`, etc.
#' @param parameter [Numeric] Parameter for the hydrological model.
#' @param inputsModel [List] Input data according to `CreateInputsModel`. The data has to start minimum 2 year + lag_days before the forecast date 
#' @param inputsPert [List] Perturbed forcing data according to `CreateInputsPert`.
#' @param basinObsTS [Data Frame] Observed time series data which has to start minimum 2 year + lag_days before the forecast date with the following columns: 
#' `date` [Date], `Temp` [Numeric] in Celsius, `Ptot` [Numeric] in mm/day, `Qmm` [Numeric] in mm/day, `PET` [Numeric] in mm/day.
#' @param DaMethod [Character] Data assimilation method, e.g., `PF` for Particle Filter.
#' @param NbMbr [Numeric] Number of ensemble members.
#' @param StatePert [Character] A vector of state variable names to be perturbed via Particle Filter: 
#' `"Prod"` - level of the production store [mm], 
#' `"Rout"` - level of the routing store [mm], 
#' `"UH1"` - unit hydrograph 1 levels [mm] (not defined for the GR5J model), 
#' `"UH2"` - unit hydrograph 2 levels [mm].
#'
#' @return [Data Frame] The output includes the date (starting one day after the `forecast_date` until the defined forecast horizon), 
#' `sd_Qsim` [Numeric] (the standard deviation of the simulated discharge), and quantiles ranging from `Q5` [Numeric] (5% quantile) to `Q95` [Numeric] in 5% steps.
get_hindcast <- function(forecast_date,
                         forecast_mode,
                         lag_days = 180,
                         Basin_Info,
                         FUN_MOD,
                         parameter,
                         inputsModel,
                         inputsPert,
                         basinObsTS, 
                         DaMethod, 
                         NbMbr, 
                         StatePert) {
  
  modes <- c("daily", "pentad", "decad")
  
  # check that the forecast_mode is either daily or pendadal 
  if (!forecast_mode %in% modes) {
    stop("Error: forecast_mode must be either 'daily' or 'pentad' or 'decad'.")
  }
  # if the forecast_mode is daily, then the forecast_end is 14 days after the forecast_date
  if (forecast_mode == "daily") {
    # Calculate forecast_end based on the maximum date in basinObsTS
    forecast_end <- as.Date(max(basinObsTS$date))
    
    # Calculate the expected forecast end date
    expected_forecast_end <- forecast_date + 14
    
    # Check if forecast_end is earlier than the expected forecast end date
    if (forecast_end < expected_forecast_end) {
      warning("Please download the recent forecast.")
    }
  }
  
  
  if (forecast_mode == "pentad") {
    forecast_end <- get_forecast_end_pentad(forecast_date)
  }
  
  if (forecast_mode == "decad") {
    forecast_end <- get_forecast_end_decad(forecast_date)
  }
  
  Date_6_months_ago <- forecast_date - lag_days
  # Check if the inputdata is long enough: 
  startDateInputModel <- min(as.Date(inputsModel$DatesR))
  min_date <- Date_6_months_ago - 2*365 
  if (min_date <= startDateInputModel) {
    stop("Error: The input data is too short. The minimum date hast to be 2 year + defined lag days before the forecast date.")
  }
  
  
  
  start_op <- format((Date_6_months_ago), format = "%Y%m%d")
  indRun_OL <- seq(from = which(format(as.Date(basinObsTS$date), format = "%Y%m%d") == start_op), 
                   to   = which(format(as.Date(basinObsTS$date), format = "%Y%m%d") == start_op))

  indRun_DA <- seq(from = which(format(basinObsTS$date, format = "%Y-%m-%d") == format(Date_6_months_ago, format = "%Y-%m-%d")),
                   to   = which(format(basinObsTS$date, format = "%Y-%m-%d") == format(forecast_end, format = "%Y-%m-%d")))
  
  
 
  warmup <- c((indRun_OL[1]-2*365):(indRun_OL[1]-1))
  runOptions_OL <- airGR::CreateRunOptions(FUN_MOD = FUN_MOD,
                                           InputsModel = inputsModel,
                                           IndPeriod_Run = indRun_OL,
                                           IniStates        = NULL,
                                           IniResLevels     = NULL,
                                           IndPeriod_WarmUp = warmup,
                                           MeanAnSolidPrecip = Basin_Info$MeanAnSolidPrecip,
                                           IsHyst = FALSE,
                                           verbose = FALSE, 
                                           RelIce = Basin_Info$rel_ice)
  
  runResults_OL <- FUN_MOD(InputsModel = inputsModel,
                           RunOptions  = runOptions_OL,
                           Param       = parameter)
  initstate <- runResults_OL$StateEnd
  
  RunOptions_DA <- airGR::CreateRunOptions(FUN_MOD = FUN_MOD,
                                           InputsModel = inputsModel,
                                           IndPeriod_Run = 1L,
                                           warning = FALSE,
                                           verbose = FALSE,
                                           IndPeriod_WarmUp = 0L,
                                           IniStates = initstate, 
                                           RelIce = Basin_Info$rel_ice)
  
  ResPF <- RunModel_DA(InputsModel = inputsModel,
                       InputsPert = inputsPert,
                       Qobs = basinObsTS$Qmm,
                       IndRun = indRun_DA,
                       FUN_MOD = FUN_MOD,
                       Param = parameter,
                       DaMethod = DaMethod, 
                       NbMbr = NbMbr, 
                       StatePert = StatePert, 
                       RunOptionsInitial = RunOptions_DA, 
                       Rel_ice = Basin_Info$rel_ice,
                       Seed = 40)
  hindcast <- calculate_stats_hindcast(ResPF, forecast_date)
  return(hindcast)
}


calculate_stats_hindcast <- function(Result_DA, 
                                     forecast_date) {

Qsim_data <- as_tibble(Result_DA$QsimEns) %>%
  mutate(date = as.Date(Result_DA$DatesR)) %>%
  pivot_longer(cols = -date, names_to = "Member", values_to = "Qsim") %>%
  filter(date > as.Date(forecast_date))

forecast_statistics <- Qsim_data %>%
  group_by(date) %>%
  summarize(
    sd_Qsim = sd(Qsim, na.rm = TRUE),
    Q5 = quantile(Qsim, probs = 0.05, na.rm = TRUE),
    Q10 = quantile(Qsim, probs = 0.10, na.rm = TRUE),
    Q15 = quantile(Qsim, probs = 0.15, na.rm = TRUE),
    Q20 = quantile(Qsim, probs = 0.20, na.rm = TRUE),
    Q25 = quantile(Qsim, probs = 0.25, na.rm = TRUE),
    Q30 = quantile(Qsim, probs = 0.30, na.rm = TRUE),
    Q35 = quantile(Qsim, probs = 0.35, na.rm = TRUE),
    Q40 = quantile(Qsim, probs = 0.40, na.rm = TRUE),
    Q45 = quantile(Qsim, probs = 0.45, na.rm = TRUE),
    Q50 = quantile(Qsim, probs = 0.50, na.rm = TRUE),
    Q55 = quantile(Qsim, probs = 0.55, na.rm = TRUE),
    Q60 = quantile(Qsim, probs = 0.60, na.rm = TRUE),
    Q65 = quantile(Qsim, probs = 0.65, na.rm = TRUE),
    Q70 = quantile(Qsim, probs = 0.70, na.rm = TRUE),
    Q75 = quantile(Qsim, probs = 0.75, na.rm = TRUE),
    Q80 = quantile(Qsim, probs = 0.80, na.rm = TRUE),
    Q85 = quantile(Qsim, probs = 0.85, na.rm = TRUE),
    Q90 = quantile(Qsim, probs = 0.90, na.rm = TRUE),
    Q95 = quantile(Qsim, probs = 0.95, na.rm = TRUE)
  )

return(forecast_statistics)
}



# gets the pentade in a year for a given date
# Attention: when giving the forecast date it gives the pentad of this date (so not the pentad of the forecasted period)
get_pentad <- function(date) {
  # If date is a vector, the function will return a vector of pentad values
  sapply(date, function(d) {
    day <- day(d)
    month <- month(d)
    year <- year(d)
    month_last <- last_day_of_month(year, month) |> day()
    
    pentad_start <- c(1, 6, 11, 16, 21, 26)
    pentad_end <- c(5, 10, 15, 20, 25, month_last)
    pentad <- seq(1, 6, by = 1)
    
    pentad_value <- NA
    for(i in pentad) {
      if(day >= pentad_start[i] & day <= pentad_end[i]) {
        pentad_value <- i + ((month - 1) * 6)
        break
      }
    }
    return(pentad_value)
  })
}

# gets the decad in a year for a given date
# Attention: when giving the forecast date it gives the decad of this date (so not the decad of the forecasted period)
get_decad <- function(date) {
  # If date is a vector, the function will return a vector of pentad values
  sapply(date, function(d) {
    day <- day(d)
    month <- month(d)
    year <- year(d)
    month_last <- last_day_of_month(year, month) |> day()
    
    decad_start <- c(1, 11, 21)
    decad_end <- c(10, 20, month_last)
    decad <- seq(1, 3, by = 1)
    
    decad_value <- NA
    for(i in decad) {
      if(day >= decad_start[i] & day <= decad_end[i]) {
        decad_value <- i + ((month - 1) * 3)
        break
      }
    }
    return(decad_value)
  })
}


# gets the last day of the month in a year
last_day_of_month <- function(year, month) {
  Date <- as.Date(paste(year, month, "05", sep="-"))
  last_day <- ceiling_date(Date, "month") - days(1)
  return(last_day)
}

# input: forecast date = pentad start - 1d
get_forecast_end_pentad <- function(forecast_date){
  # need end_forecast
  # if forecastdate day is 31 then it is 05 
  if (day(forecast_date) == 25){
    pentad_end <- last_day_of_month_date(forecast_date)
  } else {
    pentad_end <- forecast_date + 5
  }
  return(pentad_end)
}

# input: forecast date = pentad start - 1d
get_forecast_end_decad <- function(forecast_date){
  # need end_forecast
  # if forecastdate day is 31 then it is 05 
  if (day(forecast_date) == 20){
    pentad_end <- last_day_of_month_date(forecast_date)
  } else {
    pentad_end <- forecast_date + 10
  }
  return(pentad_end)
}


# Input: date, 
# Output the last day of the month of the input date
last_day_of_month_date <- function(Date) {
  last_day <- ceiling_date(Date, "month") - days(1)
  return(last_day)
}

# gets the forecasting date from a input period for pentdadal timestep
pentadal_days <- function(start_date, end_date) {
  # Convert input dates to Date class
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  
  # Initialize an empty character vector to store pentadal days
  pentadal_dates <- c()
  
  current_date <- start_date
  while (current_date <= end_date) {
    day <- day(current_date)
    if (day %in% c(5, 10, 15, 20, 25) || current_date == last_day_of_month_date(current_date)) {
      pentadal_dates <- c(pentadal_dates, as.character(current_date))
    }
    current_date <- current_date + days(1)
  }
  
  return(as.Date(pentadal_dates))
}

# gets the forecasting date from a input period for decadal timestep
decadal_days <- function(start_date, end_date) {
  # Convert input dates to Date class
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  
  # Initialize an empty character vector to store pentadal days
  pentadal_dates <- c()
  
  current_date <- start_date
  while (current_date <= end_date) {
    day <- day(current_date)
    if (day %in% c(10,20) || current_date == last_day_of_month_date(current_date)) {
      pentadal_dates <- c(pentadal_dates, as.character(current_date))
    }
    current_date <- current_date + days(1)
  }
  
  return(as.Date(pentadal_dates))
}

