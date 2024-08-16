
# convert daily forecast to pentadal and decadal 
convert_daily_to_pentad_decad <- function(forecast_data, time_steps) {
  forecast_data %>%
    dplyr::filter(forecast_date %in% time_steps) %>%
    group_by(forecast_date) %>%
    summarise(Qsim = mean(median_Qsim))
}


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
  
  if (forecast_mode == "pentadal") {
    time_steps <- pentadal_days(start_date, end_date)
  } else if (forecast_mode == "daily") {
    time_steps <- seq(from = start_date, to = end_date, by = "days")
  }
  
  inputsModel <- airGR::CreateInputsModel(FUN_MOD   = FUN_MOD,
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
    if (forecast_mode == "pentadal") {
      hindcast <- hindcast %>% 
        summarise(Qsim = mean(median_Qsim)) %>%
        mutate(forecast_date = forecast_date, 
               pentad = get_pentad(forecast_date+1)) %>%
        dplyr::select(forecast_date, Qsim, pentad)
      
    } else if (forecast_mode == "daily") {
      hindcast <- hindcast %>%
        mutate(forecast_date = forecast_date) %>% 
        dplyr::select(forecast_date, everything())
    }
    hindcast_all[[i]] <- hindcast
  }
  
  hindcast_all_df <- do.call(rbind, hindcast_all)
  return(hindcast_all_df)
}



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
  
  modes <- c("daily", "pentadal")
  
  # check that the forecast_mode is either daily or pendadal 
  if (!forecast_mode %in% modes) {
    stop("Error: forecast_mode must be either 'daily' or 'pentadal'.")
  }
  # if the forecast_mode is daily, then the forecast_end is 14 days after the forecast_date
  if (forecast_mode == "daily") {
    forecast_end <- forecast_date + 14
  }
  
  if (forecast_mode == "pentadal") {
    forecast_end <- get_forecast_end(forecast_date)
  }
  
  Date_6_months_ago <- forecast_date - lag_days
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
    median_Qsim = median(Qsim, na.rm = TRUE),
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

# get_pentad <- function(date){
#   day <- day(date)
#   month <- month(date)
#   year <- year(date)
#   month_last <- last_day_of_month(year,month) |> day()
#   
#   pentad_start <- c(1,6,11,16,21,26)
#   pentad_end <- c(5,10,15,20,25,month_last)
#   pentad <- seq(1,6, by =1)
#   
#   pentad_value <- NA
#   for(i in pentad){
#     if(day >= pentad_start[i] & day <= pentad_end[i]){
#       pentad_value <- i+((month-1)*6)
#       break
#     }
#   }
#   return(pentad_value)
# } 

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
get_forecast_end <- function(forecast_date){
  pentad_start <- forecast_date + 1
  # need end_forecast
  # if forecastdate day is 31 then it is 05 
  if (day(forecast_date) == 25){
    pentad_end <- last_day_of_month_date(forecast_date)
  } else {
    pentad_end <- forecast_date + 5
  }
  return(pentad_end)
}




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

