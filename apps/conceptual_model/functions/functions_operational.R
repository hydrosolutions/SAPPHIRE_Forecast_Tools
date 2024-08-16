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
  
  
  if ((Date_6_months_ago <= Enddate_op)) {
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
      print("Glacier model")
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
      print("Non-glacier model")
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
  
  RunOptionsIni <- airGR::CreateRunOptions(FUN_MOD = FUN_MOD,
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

# This function generates a hydrological forecast over the forecast period from  using
# both control and ensemble forecasts. It processes the forecast for each ensemble member
# and combines the results into a single output.
#
# Parameters:
# - forecast_date: [Date] The starting date for the forecast.
# - Basin_Info: [List]  A list containing information about the basin, such as hypsometric data,
#                     gradients (GradT, k-value), and ice-related parameters (rel_ice).
# - inputsModel_cf: [List]  Inputs for the control forecast model.
# - BasinObs_cf: [Data Frame] Forcing data for the control forecast period, including date, Ptot,
#                             PET, Temp.
# - BasinObs_pf: [List of Data Frames]  A list of forcing data for the ensemble forecast period,
#                                      with each list element corresponding to a different ensemble member.
# - FUN_MOD: [Function]  The hydrological model function to be used for the simulation (example: RunModel_CemaNeigeGR4J_Glacier or RunModel_CemaNeigeGR6J).
# - Result_DA: [List] The results from the data assimilation, including initial states for each ensemble member.
# - parameter: [Numeric Vector] The parameter set to be used for the model run.
#
# Function Workflow:
# 1. Set up the forecast period: Defines the start and end dates for the forecast period based on the 
#    provided forecast date and forcing data.
# 2. Create indices for the control and ensemble forecast periods: Indexes the relevant periods within 
#    the provided observation data. They <re different indexes because the control forcing datatable contains also past forcing data 
# 3. Run Control Forecast: Iteratively runs the control forecast for each ensemble member, initializing 
#    the model with the states from the data assimilation results.
# 4. Combine Control Forecast Results: Combines the individual control forecasts into a single dataframe.
# 5. Run Ensemble Forecast: Iteratively runs the ensemble forecast, processing each ensemble member separately.
# 6. Combine Ensemble Forecast Results: Combines the individual ensemble forecasts into a single dataframe.
# 7. Return Forecast: Merges the control and ensemble forecast results into a single output dataframe.
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
  
  # round two digits after the comma
  # forecast_statistics <- forecast_statistics %>%
  #   mutate(across(where(is.numeric), ~round(., 2)))
  
  return(forecast_statistics)
}




process_dataset <- function(ResPF) {
  QsimEns <- ResPF$QsimEns
  DatesR <- ResPF$DatesR
  
  ResPF_Q <- QsimEns %>%
    as_tibble() %>%
    dplyr::mutate(date = as.Date(DatesR))
  
  # Compute mean and standard deviation of Q by Date
  ResPF_Q_summary <- ResPF_Q %>%
    pivot_longer(cols = -date, names_to = "member", values_to = "Q") %>%
    group_by(date) %>%
    summarise(Q_mean = mean(Q, na.rm = TRUE),  
              Q_median = median(Q, na.rm = TRUE),
              Q_sd = sd(Q, na.rm = TRUE), 
              Q05 = quantile(Q, 0.05, na.rm = TRUE),
              Q95 = quantile(Q, 0.95, na.rm = TRUE))      
  
  return(ResPF_Q_summary)
}

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
                      PET = PE_Oudin(JD = JD, Temp = Temp, Lat = Lat, LatUnit = "rad"))
    }
    
  }
  return(basinObs)
}

################################### INITIAL SETUP ###################################



# Summary: This code formats the ERA5-Land quantile-mapped data for the airGR hydrological model and calculates PET (Oudin). 
# Input: 
# data_dir: [string] Direction to saved excel: The input Excel file saved for, for example, Kyrgyzstan, includes one date column (day.month.year, example: 01.01.2000) and then columns for the station code, for example, 15194 and 15194.1. The .1 stands for precipitation in mm/day, and without it is the temperature in degrees Celsius. This data is ERA5-Land data, quantile mapped with CEHLSA data from 1979 to 2016. 
# 
# Basin_code: [numeric]The code filters for the Basin Code and then uses Oudin to calculate PET from the temperature. It then formats the data for the airGR hydrological model.
# Basin_lat: [numeric] Latitude in radians of the centroid of the basin
# Q_obs: [data frame] with the columns 
# date (Date): The date of the observation in YYYY-MM-DD format. 
# Qmm (numeric): The discharge in millimeters, which is merged with the ERA5 data.

# Output: 
# basinObsTS: [data frame] with the columns
# date (Date): The date of the observation in YYYY-MM-DD format.
# Temp (numeric): The temperature in degrees Celsius.
# Ptot (numeric): The precipitation in millimeters per day.
# Qmm (numeric): The discharge in millimeters per day.
# PET (numeric): The potential evapotranspiration in millimeters per day.

process_ERA5QM <- function(data_dir, Basin_code, Basin_lat, Q_obs) {
  # Load the CSV file
  ERA5QM <- read_csv(data_dir, show_col_types = FALSE)
  
  # Remove the first column
  ERA5QM <- ERA5QM[,-1]
  
  # Convert to long format
  ERA5QM_long <- ERA5QM %>%
    pivot_longer(
      cols = -date, 
      names_to = "code", 
      values_to = "value") %>%
    dplyr::filter(code %in% c(as.character(Basin_code), paste0(as.character(Basin_code), ".1")))
  
  
  ERA5QM_wide <- ERA5QM_long %>%
    pivot_wider(
      names_from = code, 
      values_from = value
    ) %>%
    rename(
      Temp = as.character(Basin_code),
      Ptot = paste0(as.character(Basin_code), ".1")
    ) %>%
    left_join(Q_obs[, c("date", "Qmm")], by = "date") %>%
    dplyr::filter(date >= as.Date("2000-01-01")) %>%
    arrange(date)
  
  # Calculate PET using the PE_Oudin function
  PET_ERA5QM <- PE_Oudin(
    JD = as.POSIXlt(ERA5QM_wide$date)$yday + 1,
    Temp = ERA5QM_wide$Temp,
    Lat = Basin_lat, 
    LatUnit = "rad"
  )
  
  # Prepare the final time series data frame
  basinObsTS <- ERA5QM_wide
  basinObsTS$date <- as.POSIXct(ERA5QM_wide$date, format = "%Y%m%d", tz = "UTC")
  basinObsTS$PET <- PET_ERA5QM
  
  return(basinObsTS)
}
