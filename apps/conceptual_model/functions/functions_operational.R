



process_save_time_steps <- function(time_steps, time_type, start_date, forecast_date, dir_Results, Basin_Info, new_forecast) {
  file_path <- paste0(dir_Results, "/data/", time_type, "_", Basin_Info$BasinCode, ".csv")
  existing_data <- try(read_csv(file_path, show_col_types = FALSE), silent = TRUE)
  
  if (inherits(existing_data, "try-error") || all(is.na(existing_data))) {
    new_data <- convert_daily_to_pentad_decad(new_forecast, time_steps,time_type)
    write.csv(new_data, file_path, row.names = FALSE)
  } else {
    existing_data <- existing_data %>%
      mutate(forecast_date = as.Date(forecast_date, format = "%d.%m.%Y"))
    total_data <- rbind(existing_data, convert_daily_to_pentad_decad(new_forecast, time_steps, time_type))
    write.csv(total_data, file_path, row.names = FALSE)
  }
}

# Summary: The run_model_without_DA function executes a hydrological model run, either with or without glacier modeling, depending on the specified model function (FUN_MOD). 
# This model runs until the forecast day minus 180 days (default, cab be defined by lag_days), then saves the output.
# This output is needed for the next forecast run and is used as the starting point.
# The function checks when the last model run was saved, and if it was today, it just loads that one. This makes it possible to run the model multiple times a day.

# Input: 
# forecast_date: [Date] The date when a forecast should be made
# FUN_MOD: [function] can be either with glacier module (RunModel_CemaNeigeGR4J_Glacier) or without (RunModel_CemaNeigeGR6J)
# inputsModel: created with CreateInputsModel
# MeanAnSolidPrecip: from CreateRunOptions (default is NULL and is calculated internally) 
# RelIce: [numeric] vector of relative ice area in the elevation band. Only necessary if glacier module is used. 
# dir_Output: [string] Specify the path where the output should be stored, and also where the output from the last run (or the initial run if its the first run) was stored.
# FUN_MOD: [function] can be either with glacier module (RunModel_CemaNeigeGR4J_Glacier) or without (RunModel_CemaNeigeGR6J)
# parameter [numeric]: vector of parameter to run the hydrological model. 

# Output: 
# Result from the run of the hydrological model from the previous time the operational forecast was called - 180 days to the run to the forecast date - 180 days. 
# The function also saves the result in the dir_Output
runModel_withoutDA <- function(forecast_date,
                               FUN_MOD, 
                               Basin_Info,
                               inputsModel,
                               lag_days = 180,
                               dir_Output, 
                               parameter) {
  
  load(file.path(dir_Output, "runResults_op.RData"))
  Enddate_op <- as.Date(max(runResults_op$DatesR))
  StateEnd_op <- runResults_op$StateEnd
  
  # convert to date format
  forecast_date <- as.Date(forecast_date)
  Date_6_months_ago <- forecast_date - lag_days
  
  FUN_MODGlacierList <- c("RunModel_CemaNeigeGR4J_Glacier",
                          "RunModel_CemaNeigeGR6J_Glacier")
  
  
  if (Enddate_op > Date_6_months_ago) {
    stop("Please rerun the script : run_initial.R")
  }
  
  
  print(paste0("Enddate_op: ", Enddate_op))
  
  
  if ((Date_6_months_ago == Enddate_op)) {
    print("Multiple runs on the same day")
  } else {
    print("This is the first run of today")
    start_op <- format((Enddate_op+1), format = "%Y%m%d")
    end_op <- format(Date_6_months_ago, format = "%Y%m%d")

    
    indRun <- seq(
      from = which(format(as.Date(inputsModel$DatesR), format = "%Y%m%d") == start_op), 
      to   = which(format(as.Date(inputsModel$DatesR), format = "%Y%m%d") == end_op)
    )

    if (any(sapply(c(FUN_MODGlacierList), function(x) identical(FUN_MOD, match.fun(x))))) {
      runOptions <- airGR::CreateRunOptions(
        FUN_MOD = FUN_MOD,
        InputsModel = inputsModel,
        IndPeriod_Run = indRun,
        IniStates = StateEnd_op,
        IndPeriod_WarmUp = 0L,
        MeanAnSolidPrecip = Basin_Info$MeanAnSolidPrecip,
        IsHyst = FALSE,
        verbose = FALSE,
        RelIce = Basin_Info$rel_ice
      )
      
      runResults_op <- FUN_MOD(
        InputsModel = inputsModel,
        RunOptions = runOptions,
        Param = parameter
      )
      
    } else {
      runOptions <- airGR::CreateRunOptions(
        FUN_MOD = FUN_MOD,
        InputsModel = inputsModel,
        IndPeriod_Run = indRun,
        IniStates = StateEnd_op,
        IndPeriod_WarmUp = 0L,
        MeanAnSolidPrecip = Basin_Info$MeanAnSolidPrecip,
        IsHyst = FALSE,
        verbose = FALSE
      )
      
      runResults_op <- FUN_MOD(
        InputsModel = inputsModel,
        RunOptions = runOptions,
        Param = param
      )
    }
    save(runResults_op, file = file.path(dir_Output, "runResults_op.RData"))
  }
  return(runResults_op)
}




# Summary: Runs the model from the previous step using runModel_withoutDA and takes this as input, using it as the starting point. 
# It then runs up to the date when the forecast is triggered. 
# It perturbs the forcing input based on the fractional error parameter eps (default 0.65).
# 
# Inputs:
# - forecast_date: The date for which the forecast is to be generated.
# - Basin_Info: A list containing information about the basin, such as hypsometric data, 
#               gradients, and ice-related parameters.
# - basinObs: A data frame of observed data including precipitation (Ptot), potential 
#             evapotranspiration (PET), temperature (Temp), and observed discharge (Qmm).
# - FUN_MOD: The hydrological model function to be used for the simulation (RunModel_CemaNeigeGR4J_Glacier, RunModel_CemaNeigeGR6J)
# - runResults: Results from a previous model run, containing the final states (StateEnd) 
#               and corresponding dates (DatesR).
# - inputsModel: A list of model inputs required by the hydrological model function. For the control forecast 
# - parameter: The parameter set to be used for the model run.
# - NbMbr: The number of ensemble members for the data assimilation process.
# - DaMethod: The data assimilation method to be used (PF).
# - StatePert: Perturbation applied to the state variables for example c("Rout", "Prod") for Routing and Produciton store. 
# - eps: [numeric] The perturbation magnitude (fractional error parameter) for precipitation (Ptot) and potential evapotranspiration (PET). 
# 
# Function Workflow:
# 1. Define Operation End Date: Identifies the end date of the previous operation period 
#    from runResults and initializes the states from the end of the previous run.
# 2. Set Up Running Period: Calculates the period for which the data assimilation needs 
#    to be applied, based on forecast_date and the end date of the previous operation.
# 3. Create Perturbed Inputs: Calls CreateInputsPert to generate perturbed inputs for 
#    the model, accounting for uncertainties in precipitation, evapotranspiration, and temperature.
# 4. Initialize Run Options: Sets the initial run options, including model states and 
#    other configurations, using airGR::CreateRunOptions.
# 5. Run the Model with Data Assimilation: Calls RunModel_DA to perform the model run with 
#    data assimilation, using the perturbed inputs and other provided parameters.
# 6. Return Results: Returns the result of the data assimilation process, which includes 
#    updated state estimates and possibly other outputs depending on the DA method used.

runModel_withDA <- function(forecast_date, 
                            Basin_Info,
                            basinObs,
                            FUN_MOD,
                            runResults, 
                            inputsModel,
                            parameter,
                            NbMbr, 
                            DaMethod, 
                            StatePert, 
                            eps = 0.65) {

  Enddate_op <- as.Date(max(runResults$DatesR))
  initstate <- runResults$StateEnd
  
  # Runnung period 
  start_DA <- (Enddate_op+1) %>% format(format = "%Y%m%d")
  end_DA <- as.Date(forecast_date) %>% format(format = "%Y%m%d")
  indRun_DA <- seq(from = which(format(basinObs$date, format = "%Y%m%d") == start_DA), 
                   to   = which(format(basinObs$date, format = "%Y%m%d") == end_DA))
  
  
  InputsPert <- CreateInputsPert(FUN_MOD = FUN_MOD,
                                 DatesR = basinObs$date,
                                 Precip = basinObs$Ptot,
                                 PotEvap = basinObs$PET,
                                 TempMean  = basinObs$Temp,
                                 HypsoData = Basin_Info$HypsoData,
                                 ZInputs   = median(Basin_Info$HypsoData),
                                 NbMbr = NbMbr,
                                 Seed = 40,
                                 Eps_Ptot = eps, 
                                 Eps_PET = eps,
                                 GradP = Basin_Info$k_value, 
                                 GradT = Basin_Info$GradT)
  
  RunOptionsIni <- CreateRunOptions(FUN_MOD = FUN_MOD,
                                           InputsModel = inputsModel,
                                           IndPeriod_Run = 1L,
                                           warning = FALSE, verbose = FALSE,
                                           IniStates = initstate, 
                                           IndPeriod_WarmUp = 0L, 
                                           RelIce = Basin_Info$rel_ice)
  
  Result_DA <- RunModel_DA(InputsModel = inputsModel,
                       InputsPert = InputsPert,
                       Qobs = basinObs$Qmm,
                       IndRun = indRun_DA,
                       FUN_MOD = FUN_MOD,
                       Param = parameter,
                       DaMethod = DaMethod, 
                       NbMbr = NbMbr, 
                       StatePert = StatePert, 
                       RunOptionsInitial = RunOptionsIni, 
                       Rel_ice = Basin_Info$rel_ice,
                       Seed = 40)
  return(Result_DA)
}


#' Generate Hydrological Forecasts Using Control and Ensemble Forecasts
#'
#' This function generates hydrological forecasts over a specified forecast period using both control and ensemble forecasts. 
#' It processes the forecast for each ensemble member, running the hydrological model, and combines the results into a single output data frame.
#'
#' @param forecast_date [Date] The starting date for the forecast.
#' @param Basin_Info [List] A list containing information about the basin, such as hypsometric data, gradients (GradT, k-value), and ice-related parameters (rel_ice).
#' @param inputsModel_cf [List] Inputs for the control forecast model.
#' @param BasinObs_cf [Data Frame] Forcing data for the control forecast period, including date, Ptot, PET, and Temp.
#' @param BasinObs_pf [List of Data Frames] A list of forcing data for the ensemble forecast period, with each list element corresponding to a different ensemble member.
#' @param FUN_MOD [Function] The hydrological model function to be used for the simulation (e.g., RunModel_CemaNeigeGR4J_Glacier or RunModel_CemaNeigeGR6J).
#' @param Result_DA [List] The results from data assimilation, including initial states for each ensemble member.
#' @param parameter [Numeric Vector] The parameter set to be used for the model run.
#'
#' @return Data Frame. A data frame containing the combined forecast results from both the control and ensemble forecasts, with columns:
#'   - `date`: The date of the forecast.
#'   - `Ensemble`: The ensemble member identifier, including control forecasts. Example: Mbr_1_ForecastEns_cf, where MBR stands for the ensmeble member in the data assimilation and ForecastEns stands for the control member (cf) for the forecast or a number for the perturbed ensmeble 
#'   - `Qsim`: The simulated discharge for each date and ensemble member.
#'
#' @details 
#'   - The function first sets up the forecast period and creates indices for the control and ensemble forecast periods. These indeces have the same length, but the control member forcing starts earlier resulting in other indixes. 
#'   - It then runs the control forecast by initializing the model with states from the data assimilation results for each ensemble member.
#'   - The control forecast results are combined into a single data frame.
#'   - Next, the function runs the ensemble forecast for each ensemble member, processes the results, and combines them into a data frame.
#'   - Finally, the control and ensemble forecast results are merged into a single output data frame.
#'
#' @export
runModel_ForecastPeriod <- function(forecast_date, 
                                    Basin_Info,
                                    inputsModel_cf,
                                    BasinObs_cf,
                                    BasinObs_pf,
                                    FUN_MOD,
                                    Result_DA,
                                    parameter) {
  
  start <- format(forecast_date + 1, format = "%Y%m%d")
  end <- format(max(BasinObs_cf$date), format = "%Y%m%d")
  
  
  # Create index of run periods: is different because BasinObs_cf has whole timer series including historical data 
  indRun_cf <- which(format(BasinObs_cf$date, format = "%Y%m%d") == start):which(format(BasinObs_cf$date, format = "%Y%m%d") == end)
  indRun_pf <- which(format(BasinObs_pf[[1]]$date, format = "%Y%m%d") == start):which(format(BasinObs_pf[[1]]$date, format = "%Y%m%d") == end)
  
  
  NbMbr <- Result_DA$NbMbr
  No_Nb_ens <- length(BasinObs_pf)
  
  ####### CONTROL FORECAST ########
  runResults_forecast_cf <- list()
  for (i in 1:NbMbr) {
    Initial_ens <- Result_DA$IniStatesEns[[i]]
    
    runOptions <- CreateRunOptions(FUN_MOD = FUN_MOD,
                                   InputsModel = inputsModel_cf,
                                   IndPeriod_Run = indRun_cf,
                                   IniStates = Initial_ens,
                                   IndPeriod_WarmUp = 0L,
                                   MeanAnSolidPrecip = Basin_Info$MeanAnSolidPrecip,
                                   IsHyst = FALSE,
                                   verbose = FALSE, 
                                   RelIce = Basin_Info$rel_ice)
    
    
    runResults_forecast_cf[[i]] <- FUN_MOD(InputsModel = inputsModel_cf,
                                           RunOptions = runOptions,
                                           Param = parameter)
    
  }
  # Combine individual forecasts into a single dataframe
  runResults_forecast_cf_df <- bind_rows(lapply(seq_along(runResults_forecast_cf), function(i) {
    tibble(
      date = as.Date(runResults_forecast_cf[[i]]$DatesR),
      Ensemble = paste("Mbr", i,"ForecastEns","cf", sep = "_"),
      Qsim = runResults_forecast_cf[[i]]$Qsim
    )
  }))
  
  ########## ENSEMBLE FORECAST 
  runResults_forecast_pf <- vector("list", NbMbr) # Create a list to hold lists
  for (i in 1:NbMbr) {
    Initial_ens <- ResPF$IniStatesEns[[i]]
    
    runResults_forecast_pf[[i]] <- vector("list", (No_Nb_ens)) # Create a sub-list for each ensemble member
    
    for (j in 1:(No_Nb_ens)) {
      
      inputsModel_pf <- airGR::CreateInputsModel(FUN_MOD   = FUN_MOD,
                                                 DatesR    = BasinObs_pf[[j]]$date,
                                                 Precip    = BasinObs_pf[[j]]$Ptot,
                                                 PotEvap   = BasinObs_pf[[j]]$PET,
                                                 TempMean  = BasinObs_pf[[j]]$Temp,
                                                 HypsoData = Basin_Info$HypsoData,
                                                 ZInputs   = median(Basin_Info$HypsoData),
                                                 verbose = FALSE,
                                                 GradP = Basin_Info$k_value,
                                                 GradT = Basin_Info$GradT)
      
      runOptions_pf <- CreateRunOptions(FUN_MOD = FUN_MOD,
                                        InputsModel = inputsModel_pf,
                                        IndPeriod_Run = indRun_pf,
                                        IniStates = Initial_ens,
                                        IndPeriod_WarmUp = 0L,
                                        MeanAnSolidPrecip = Basin_Info$MeanAnSolidPrecip,
                                        IsHyst = FALSE,
                                        verbose = FALSE,
                                        RelIce = Basin_Info$rel_ice)
      
      # Execute the model
      runResults_forecast_pf[[i]][[j]] <- FUN_MOD(InputsModel = inputsModel_pf,
                                                  RunOptions = runOptions_pf,
                                                  Param = parameter)
    }
  }
  runResults_forecast_pf_df <- bind_rows(lapply(seq_along(runResults_forecast_pf), function(i) {
    bind_rows(lapply(seq_along(runResults_forecast_pf[[i]]), function(j) {
      tibble(
        date = as.Date(runResults_forecast_pf[[i]][[j]]$DatesR),
        Ensemble = paste("Mbr", i, "ForecastEns", j, sep = "_"),
        Qsim = runResults_forecast_pf[[i]][[j]]$Qsim
      )
    }))
  }))
  
  forecast <- rbind(runResults_forecast_cf_df, runResults_forecast_pf_df)
  return(forecast)
}

# Calculate statistics including quantiles
# Input: 
# Output: 
calculate_stats_forecast <- function(forecast) {
  forecast_statistics <- forecast %>%
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
  
  # round two digits after the comma
  # forecast_statistics <- forecast_statistics %>%
  #   mutate(across(where(is.numeric), ~round(., 2)))
  
  return(forecast_statistics)
}

#' Process Discharge Data for a Basin
#'
#' This function processes observed discharge data for a specified basin, converting the discharge values
#' to a standardized unit of millimeters per day (mm/day) based on the basin area.
#'
#' @param dir_Q Character. The file path to the CSV file containing the observed discharge data.
#' @param BasinCode Character. The code identifying the basin for which the data should be processed.
#' @param BasinArea_m2 Numeric. The area of the basin in square meters.
#'
#' @return 
#' A data frame with the following columns:
#'   - `date`: The date of the discharge observation (as `Date` class).
#'   - `Qmm`: The discharge per unit area, converted to millimeters per day (mm/day).
#'
#' @details 
#'   - The function reads discharge data from the specified CSV file.
#'   - It filters the data for the specified `BasinCode`.
#'   - The discharge (`discharge`) is converted from m3/s to mm/day using the basin area (`BasinArea_m2`).
process_discharge_data <- function(dir_Q, BasinCode, BasinArea_m2) {
  Q_obs <- read_csv(dir_Q, show_col_types = FALSE)
  
  # Perform data transformation
  Q_obs <- Q_obs %>%
    # Filter for the specified basin
    dplyr::filter(code == BasinCode) %>%
    dplyr::mutate(date = as.Date(date, format = "%Y-%m-%d"), 
                  Qmm = discharge / BasinArea_m2 * 1000 * 60 * 60 * 24) # Calculate discharge per unit area and convert to mm/day
    
  
  return(Q_obs)
}

#' Process Meteorological Forcing Data for a Basin
#'
#' This function processes temperature and precipitation data for a specified basin. It can handle
#' either a control forecast ("cf") or multiple ensemble members given as numeric vector (example c(1:50). The function calculates the potential 
#' evapotranspiration (PET) using the Oudin method and optionally merges the data with observed streamflow.
#'
#' @param member_id Specifies the type of forecast:
#'   - `"cf"`: [string] Control forecast.
#'   - Ensemble member IDs: [numeric] vector of numeric numbers for the ensmeble member. 
#' @param Basin_code [Numeric] The code identifying the basin.
#' @param file_path_Temp [Character] The file path to the temperature data CSV.
#' @param file_path_Ptot [Character] The file path to the precipitation data CSV.
#' @param Lat [Numeric] The latitude of the basin in radians.
#' @param Q_obs [Data frame] (optional). Contains observed streamflow with columns `date` and `Qmm`. If provided, it is merged with the processed data.
#'
#' @return 
#'   - If `member_id` is `"cf"`: A data frame with the following columns:
#'     - `date`: The date of the observation.
#'     - `Ptot`: The total precipitation.
#'     - `Temp`: The temperature.
#'     - `PET`: The potential evapotranspiration calculated using the Oudin method.
#'     - `Qmm`: The observed streamflow (if `Q_obs` is provided).
#'   - If `member_id` contains ensemble IDs: A list of data frames, each corresponding to an ensemble member, with columns `date`, `Ptot`, `Temp`, and `PET`.
#'
#' @details 
#'   - The function reads temperature and precipitation data from the specified CSV files.
#'   - If processing the control forecast (`"cf"`), the data is filtered by `Basin_code`.
#'   - For ensemble forecasts, data is processed separately for each ensemble member.
#'   - PET is calculated using the Oudin method, which requires the day of the year (`JD`), temperature, and latitude.
#'   - If observed streamflow data (`Q_obs`) is provided, it is merged with the processed data based on the date.
process_forecast_forcing <- function(member_id,
                                     Basin_code = Basin_code,
                                     file_path_Temp,
                                     file_path_Ptot,
                                     Lat,
                                     Q_obs = NULL) {

  if (member_id[1] == "cf") {
    P_cf <- read_csv(file_path_Ptot, show_col_types = FALSE) %>%
      dplyr::filter(code == Basin_code)
    
    T_cf <- read_csv(file_path_Temp, show_col_types = FALSE) %>%
      dplyr::filter(code == Basin_code)
    

    basinObs <- data.frame(date = P_cf$date, Ptot = P_cf$P, Temp = T_cf$T) %>%
      dplyr:: mutate(date = ymd(date, tz = "UTC"),
             JD = yday(date),
             PET = PE_Oudin(JD = JD, Temp = Temp, Lat = Lat, LatUnit = "rad")) %>%
      dplyr::select(-JD) %>%
      left_join(Q_obs %>% dplyr::select(date, Qmm), by = "date")
    

    
  } else {
    P_ens <- read_csv(file_path_Ptot, show_col_types = FALSE)
    T_ens <- read_csv(file_path_Temp, show_col_types = FALSE)
    basinObs <- list()
    

    for (i in 1:length(member_id)) {
      P_member <- P_ens %>% dplyr::filter(ensemble_member == member_id[i])
      T_member <- T_ens %>% dplyr::filter(ensemble_member == member_id[i])
      basinObs[[i]] <- data.frame(date = P_member$date, Ptot = P_member$P, Temp = T_member$T) %>%
        dplyr::mutate(date = ymd(date, tz = "UTC"),
                      JD = yday(date) + 1,
                      PET = PE_Oudin(JD = JD, Temp = Temp, Lat = Lat, LatUnit = "rad")) %>%
        dplyr::select(-JD)
    }
    
  }
  return(basinObs)
}

