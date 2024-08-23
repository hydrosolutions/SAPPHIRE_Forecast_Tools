##################################################################
# Have to be in folder: cd /Users/adrian/Documents/GitHub/SAPPHIRE_Forecast_Tools/apps/conceptual_model
# run with: SAPPHIRE_OPDEV_ENV=True Rscript run_operation_forecasting_CM.R
# env file is in: /data/sensitive_data_forecast_tools/config/.env_develop_kghm
# .json file is in: /data/sensitive_data_forecast_tools/config/config_conceptual_model.json
##################################################################

library(here)
library(jsonlite)
library(devtools, quietly = TRUE)
# devtools::install_github("hydrosolutions/airGR_GM")
# devtools::install_github("hydrosolutions/airgrdatassim")
library(airGR, quietly = TRUE)
library(airGRdatassim, quietly = TRUE)
library(tidyverse, quietly = TRUE)
library(usethis, quietly = TRUE)

# 1 Configuration
# Check if this script is run from within a docker container.
# We do this by checking if an environment variable set by the Dockerfile is
# present or not.
if (Sys.getenv("IN_DOCKER_CONTAINER")=="") {
  print("Running from local machine")
  # Environment variable IN_DOCKER_CONTAINER is not set. Run from local machine
  # This code assumes that forecast_configuration has been opened in
  # apps/configuration_dashboard for development
  setwd(here())
  # setwd("apps/conceptual_model")
  print(getwd())
  if (Sys.getenv("SAPPHIRE_OPDEV_ENV")=="True") {
    if (!file.exists("../../../sensitive_data_forecast_tools/config/.env_develop_kghm")) {
      stop("File ../../../sensitive_data_forecast_tools/config/.env_develop_kghm not found. ")
    }
    readRenviron("../../../sensitive_data_forecast_tools/config/.env_develop_kghm")
  } else {
    # Test if the file .env_develop exists.
    if (!file.exists("../config/.env_develop")) {
      stop("File ../config/.env_develop not found. ")
    }
    readRenviron("../config/.env_develop")
  }
} else {
  print("Running from docker container")
  # Environment variable IN_DOCKER_CONTAINER is set. Run from docker container
  setwd("/app")
  print(getwd())
  if (Sys.getenv("SAPPHIRE_OPDEV_ENV")=="True") {
    if (!file.exists("../sensitive_data_forecast_tools/config/.env_develop_kghm")) {
      stop("File ../sensitive_data_forecast_tools/config/.env_develop_kghm not found. ")
    }
    readRenviron("../sensitive_data_forecast_tools/config/.env_develop_kghm")
  } else {
    # Test if the file .env exists.
    if (!file.exists("apps/config/.env")) {
      stop("File apps/config/.env not found. ")
    }
    readRenviron("apps/config/.env")
  }
}

# print(Sys.getenv("ieasyhydroforecast_JSON_FILE"))
 
##################################################################
# 0 Function ####
source("functions/functions_operational.R")
source("functions/functions_plot.R")
source("functions/functions_hindcast.R")


################### CHANGE ###################
forecast_date <- today()

################### INITIALIZE ###################
# JSON file
if (!dir.exists(Sys.getenv("ieasyhydroforecast_PATH_TO_JSON"))) {
  stop("Directory ", Sys.getenv("ieasyhydroforecast_PATH_TO_JSON"), " not found. ")
}
config_dir = Sys.getenv("ieasyhydroforecast_PATH_TO_JSON")
config_setup = Sys.getenv("ieasyhydroforecast_FILE_SETUP")
config <- fromJSON(paste0(config_dir,"/",config_setup))

# general path
dir_Q <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_Q"))
dir_Control <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_CF"))
dir_Ensemble <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_PF"))
dir_hindcast <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_HIND"))

# file names for control memnber (cf) and ensemble member (pf) forecast and for the hindcasting forcing data (starts from 2009-01-01)
# cf: all basins in one file
cfP_forecast_filename <- Sys.getenv("ieasyhydroforecast_FILE_CF_P")
cfT_forecast_filename <- Sys.getenv("ieasyhydroforecast_FILE_CF_T")

# hindcast filename
P_hindcast_filename <- Sys.getenv("ieasyhydroforecast_FILE_CF_HIND_P")
T_hindcast_filename <- Sys.getenv("ieasyhydroforecast_FILE_CF_HIND_T")

# Extract parameters from config
Nb_ens <- config$Nb_ens
NbMbr <- config$NbMbr
DaMethod <- config$DaMethod
StatePert <- config$StatePert
eps <- config$eps
lag_days <- config$lag_days
forecast_mode <- "daily"


forecast_date_format <- forecast_date %>%
  format(format = "%Y%m%d")


################### RUNNING ###################
for (Code in config$codes) {
  print(Code)
  
  # Step 1: Prepare Input data ####
  FUN_MOD <- get(config$fun_mod_mapping[[as.character(Code)]])
  
  # # basin specific path
  dir_basin <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_BASININFO"), Code)
  dir_Output <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_INITCOND"), Code)
  dir_Results <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_RESULT"), Code)

  ## 1.1 Initial ####
  load(file.path(dir_basin,Sys.getenv("ieasyhydroforecast_FILE_PARAM")))
  load(file.path(dir_basin,Sys.getenv("ieasyhydroforecast_FILE_BASININFO")))
  load(file.path(dir_Output, "runResults_op.RData"))
  Enddate_operational <- as.Date(max(runResults_op$DatesR))
  
  # pf: one file per basin
  pfP_forecast_filename <- paste0(Code,Sys.getenv("ieasyhydroforecast_FILE_PF_P"))
  pfT_forecast_filename <- paste0(Code,Sys.getenv("ieasyhydroforecast_FILE_PF_T"))

  ## 1.2 Discharge observations ####
  Q_obs <- process_discharge_data(file.path(dir_Q, Sys.getenv("ieasyhydroforecast_FILE_Q")), Basin_Info$BasinCode, Basin_Info$BasinArea_m2)
  
  ## 1.3 Operational forcing data  ####
  basinObs_cf <- process_forecast_forcing(member_id = "cf",
                                          Basin_code = Basin_Info$BasinCode,
                                          file_path_Ptot = file.path(dir_Control, cfP_forecast_filename),
                                          file_path_Temp = file.path(dir_Control, cfT_forecast_filename),
                                          Lat = Basin_Info$BasinLat_rad,
                                          Q_obs = Q_obs)
  
  min_basinObs_cf <- as.Date(min(basinObs_cf$date))
  
  # If the last run is a long time ago (more than 185d, for example after triggering run_initial.R)
  if (min_basinObs_cf >  max(runResults_op$DatesR)) {
    basinObs_hind <- process_forecast_forcing(member_id = "cf",
                                              Basin_code = Basin_Info$BasinCode,
                                              file_path_Ptot = file.path(dir_hindcast, P_hindcast_filename),
                                              file_path_Temp = file.path(dir_hindcast, T_hindcast_filename),
                                              Lat = Basin_Info$BasinLat_rad,
                                              Q_obs = Q_obs)
    
    # combine
    basinObs_cf <- rbind(basinObs_hind, basinObs_cf)
    basinObs_cf <- basinObs_cf %>%
      distinct(date, .keep_all = TRUE)
  }
  
  
  basinObs_pf <- process_forecast_forcing(member_id = Nb_ens,
                                          Basin_code = Basin_Info$BasinCode,
                                          file_path_Ptot = file.path(dir_Ensemble, pfP_forecast_filename),
                                          file_path_Temp = file.path(dir_Ensemble, pfT_forecast_filename),
                                          Lat = Basin_Info$BasinLat_rad)
  
  inputsModel_cf <- CreateInputsModel(FUN_MOD   = FUN_MOD,
                                             DatesR    = basinObs_cf$date,
                                             Precip    = basinObs_cf$Ptot,
                                             PotEvap   = basinObs_cf$PET,
                                             TempMean  = basinObs_cf$Temp,
                                             HypsoData = Basin_Info$HypsoData,
                                             ZInputs   = median(Basin_Info$HypsoData),
                                             verbose = FALSE,
                                             GradT = Basin_Info$GradT,
                                             GradP = Basin_Info$k_value)
  
  # Printing 
  print(paste0("Forecast day ",forecast_date))
  print(paste0("Last Q measurement: ", max(Q_obs$date)))
  print(paste0("The meterological forecast is from the ",min(basinObs_pf[[1]]$date), " until ", max(basinObs_pf[[1]]$date)))
  print(paste0("Forecast horizon control member(should be the same): ",max(basinObs_cf$date)))
  # print(paste0("The last run was at: ", max(runResults_op$DatesR)))
  # plotting
  # plot_f <- plot_ensemble_forecast( start_date = as.Date(forecast_date-15) , Parameter = "Ptot", basinObs_cf, basinObs_pf)
  # ggsave(plot_f,file = paste0(dir_Results,"/plot/Forcing_",Basin_Info$BasinCode,"_",forecast_date_format,".pdf"), width = 10, height = 6, dpi = 300)
  # plot1 <- plot_ensemble_forecast( start_date = as.Date(forecast_date-15) , Parameter = "Temp", basinObs_cf, basinObs_pf)
  

  
  
  
  # Step 2 Model run without DA ####
  runResults_op <- runModel_withoutDA(forecast_date = forecast_date,
                                      FUN_MOD = FUN_MOD,
                                      Basin_Info = Basin_Info,
                                      inputsModel = inputsModel_cf,
                                      lag_days = lag_days,
                                      dir_Output = dir_Output,
                                      parameter = param)
  
  # Step 3: Data assimilation ####
  ResPF <- runModel_withDA(forecast_date = forecast_date,
                           Basin_Info = Basin_Info,
                           basinObs = basinObs_cf,
                           FUN_MOD = FUN_MOD,
                           runResults = runResults_op,
                           inputsModel = inputsModel_cf,
                           parameter = param,
                           NbMbr = NbMbr,
                           DaMethod = DaMethod,
                           StatePert = StatePert,
                           eps = eps)
  
  # Step 4: Forecast: control and ensemble runs ####
  forecast <- runModel_ForecastPeriod(forecast_date = forecast_date,
                                      Basin_Info = Basin_Info,
                                      inputsModel_cf = inputsModel_cf,
                                      BasinObs_cf = basinObs_cf,
                                      BasinObs_pf = basinObs_pf,
                                      FUN_MOD = FUN_MOD,
                                      Result_DA = ResPF,
                                      parameter = param)
  
  # Step 5: Calculate statistics ####
  forecast_statistics <- calculate_stats_forecast(forecast)
  forecast_statistics <- forecast_statistics %>%
    mutate(forecast_date = forecast_date) %>%
    select(forecast_date, everything())
  
  # Step 6: Plotting  ####
  plot <- plot_forecast(forecast_date = forecast_date,
                        ResPF = ResPF,
                        basinObs = basinObs_cf,
                        forecast_statistics,
                        basin_name = Basin_Info$BasinName,
                        window = 30)
  

  ggsave(plot,file = paste0(dir_Results,"/plot/Overviewplot_",Basin_Info$BasinCode,"_",forecast_date_format,".pdf"), width = 10, height = 6, dpi = 300)
  
  # Step 7: Hindcast and save  ####
  ## 7.1 Daily   ####
  existing_forecasts <- try(read_csv(paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), show_col_types = FALSE), silent = TRUE)
  
  # CASE 1: file does not exist or is empty 
  if (inherits(existing_forecasts, "try-error") || all(is.na(existing_forecasts)))  {
    print("Empty file or file does not exist")
    # Save the new forecast statistics
    write.csv(forecast_statistics, paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)
    last_forecast_date <- forecast_date
    # CASE 2: file exists 
  } else {
    print("File exist")
    existing_forecasts <- existing_forecasts %>%
      mutate(forecast_date = as.Date(forecast_date, format = "%d.%m.%Y"),
             date = as.Date(date, format = "%d.%m.%Y"))
    last_forecast_date <- as.Date(max(existing_forecasts$forecast_date))
    # CASE 2.1: ;ULTIPLE RUN A DAY 
    if (forecast_date == last_forecast_date){
      print("Multiple run: overwrite the forecast from today")
      
      filtered_forecasts <- existing_forecasts %>%
        filter(forecast_date != !!forecast_date) # !! needed because of the same name of column and variable
      
      total_forecast <- rbind(filtered_forecasts, forecast_statistics)
      write.csv(total_forecast, paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)
      
      # CASE 2.2 FIRST RUN OF THE DAY WITHOUT HINDCAST 
    } else if (as.numeric(difftime(forecast_date, last_forecast_date, units = "days")) == 1) {
      print("First run of today, no hindcasting needed")
      total_forecast <- rbind(existing_forecasts, forecast_statistics)
      write.csv(total_forecast, paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)
      
      # CASE 2.3 FIRST RUN OF THE DAY WITH HINDCAST 
    } else if (as.numeric(difftime(forecast_date, last_forecast_date, units = "days")) > 1) {
      start_date_hindcast <- last_forecast_date + 1
      end_date_hindcast <- forecast_date - 1
      print(paste("There are missing forecasts from", start_date_hindcast, "to", end_date_hindcast, "-> hindcasting"))
      
      # get all the hindcasting data as well
      basinObs_hind <- process_forecast_forcing(member_id = "cf",
                                                Basin_code = Basin_Info$BasinCode,
                                                file_path_Ptot = file.path(dir_hindcast, P_hindcast_filename),
                                                file_path_Temp = file.path(dir_hindcast, T_hindcast_filename),
                                                Lat = Basin_Info$BasinLat_rad,
                                                Q_obs = Q_obs)
      
      # combine
      basinObsTS_long <- rbind(basinObs_hind, basinObs_cf)
      basinObsTS_long <- basinObsTS_long %>%
        distinct(date, .keep_all = TRUE)
      
      
      hindcast <- get_hindcast_period(start_date =  start_date_hindcast,
                                      end_date = end_date_hindcast,
                                      forecast_mode = forecast_mode,
                                      lag_days = lag_days,
                                      Basin_Info = Basin_Info,
                                      basinObsTS = basinObsTS_long,
                                      FUN_MOD = FUN_MOD,
                                      param = param,
                                      NbMbr = NbMbr,
                                      DaMethod = DaMethod,
                                      StatePert = StatePert,
                                      eps = eps)
      
      forecast_statistics <- rbind(hindcast, forecast_statistics)
      total_forecast <- rbind(existing_forecasts, forecast_statistics)
      write.csv(total_forecast, paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)
    }
  }


  
  ## 7.2 pentadal  ####
  if ((length(pentadal_days(forecast_date, forecast_date)) == 0) && !(exists("start_date_hindcast"))) {
    print("not pentadal timestep no hindcasting")
  } else if (!(length(pentadal_days(forecast_date, forecast_date)) == 0) && !(exists("start_date_hindcast"))) {
    print("Pentadal forecast day, no hindcasting")
    pentadal_steps <- pentadal_days(forecast_date, forecast_date)
    process_save_time_steps(pentadal_steps, "pentad", start_date_hindcast, forecast_date, dir_Results, Basin_Info, forecast_statistics)
    
  } else if (exists("start_date_hindcast")) {
    print("Pentadal day, hindcasting")
    pentadal_steps <- pentadal_days(start_date_hindcast, forecast_date)
    process_save_time_steps(pentadal_steps, "pentad", start_date_hindcast, forecast_date, dir_Results, Basin_Info, forecast_statistics)
  }
  
  ## 7.3 decadal  ####
  if ((length(decadal_days(forecast_date, forecast_date)) == 0) && !(exists("start_date_hindcast"))) {
    print("not decadal timestep no hindcasting")
  } else if (!(length(decadal_days(forecast_date, forecast_date)) == 0) && !(exists("start_date_hindcast"))) {
    print("Decadal forecast day, no hindcasting")
    decadal_steps <- decadal_days(forecast_date, forecast_date)
    process_save_time_steps(decadal_steps, "decad", start_date_hindcast, forecast_date, dir_Results, Basin_Info, forecast_statistics)
    
  } else if (exists("start_date_hindcast")) {
    print("Decadal day, hindcasting")
    decadal_steps <- decadal_days(start_date_hindcast, forecast_date)
    process_save_time_steps(decadal_steps, "decad", start_date_hindcast, forecast_date, dir_Results, Basin_Info, forecast_statistics)
  }
}
  

