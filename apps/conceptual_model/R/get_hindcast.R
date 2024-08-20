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
#' @export
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
    forecast_end <- forecast_date + 14
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