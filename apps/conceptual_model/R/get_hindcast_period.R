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
#' @export
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