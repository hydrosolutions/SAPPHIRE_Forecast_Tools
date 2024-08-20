##################################################################
# Have to be in folder: cd /Users/adrian/Documents/GitHub/SAPPHIRE_Forecast_Tools/apps/conceptual_model
# run with: SAPPHIRE_OPDEV_ENV=True Rscript run_initial.R
# env file is in: /data/sensitive_data_forecast_tools/config/.env_develop_kghm
# .json file is in: /data/sensitive_data_forecast_tools/config/config_conceptual_model.json
##################################################################

library(here)
library(jsonlite)
library(devtools, quietly = TRUE)
# devtools::install_github("hydrosolutions/airGR_GM", force = TRUE)
# devtools::install_github("hydrosolutions/airgrdatassim", force = TRUE)
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

start <- as.Date(config$start_ini) %>%
  format("%Y%m%d")

end <- as.Date(config$end_ini) %>%
  format("%Y%m%d")

################### RUNNING ###################
for (Code in config$codes) {


  # Step 1: Prepare Input data ####
  FUN_MOD <- get(config$fun_mod_mapping[[as.character(Code)]])
  
  # pf: one file per basin
  pfP_forecast_filename <- paste0(Code,Sys.getenv("ieasyhydroforecast_FILE_PF_P"))
  pfT_forecast_filename <- paste0(Code,Sys.getenv("ieasyhydroforecast_FILE_PF_T"))
  
  # # basin specific path
  dir_basin <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_BASININFO"), Code)
  dir_Output <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_INITCOND"), Code)
  dir_Results <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_RESULT"), Code)
  
  ## 1.1 Initial ####
  load(file.path(dir_basin,Sys.getenv("ieasyhydroforecast_FILE_PARAM")))
  load(file.path(dir_basin,Sys.getenv("ieasyhydroforecast_FILE_BASININFO")))

  ## 1.2 Discharge observations ####
  Q_obs <- process_discharge_data(file.path(dir_Q, Sys.getenv("ieasyhydroforecast_FILE_Q")), Basin_Info$BasinCode, Basin_Info$BasinArea_m2)

  ## 1.3 Operational forcing data  ####
  basinObs_cf <- process_forecast_forcing(member_id = "cf",
                                          Basin_code = Basin_Info$BasinCode,
                                          file_path_Ptot = file.path(dir_Control, cfP_forecast_filename),
                                          file_path_Temp = file.path(dir_Control, cfT_forecast_filename),
                                          Lat = Basin_Info$BasinLat_rad,
                                          Q_obs = Q_obs)
  
  basinObs_hind <- process_forecast_forcing(member_id = "cf",
                                            Basin_code = Basin_Info$BasinCode,
                                            file_path_Ptot = file.path(dir_hindcast, P_hindcast_filename),
                                            file_path_Temp = file.path(dir_hindcast, T_hindcast_filename),
                                            Lat = Basin_Info$BasinLat_rad,
                                            Q_obs = Q_obs)

  # combine
  basinObsTS <- rbind(basinObs_hind, basinObs_cf)
  basinObsTS <- basinObsTS %>%
    distinct(date, .keep_all = TRUE)

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



  indRun <- seq(from = which(format(basinObsTS$date, format = "%Y%m%d") == start),
                to = which(format(basinObsTS$date, format = "%Y%m%d") == end))

  warmup <- c((indRun[1]-1*365):(indRun[1]-1))



  runOptions <- airGR::CreateRunOptions(FUN_MOD = FUN_MOD,
                                        InputsModel = inputsModel,
                                        IndPeriod_Run = indRun,
                                        IniStates        = NULL,
                                        IniResLevels     = NULL,
                                        IndPeriod_WarmUp = warmup,
                                        MeanAnSolidPrecip = Basin_Info$MeanAnSolidPrecip,
                                        IsHyst = FALSE,
                                        RelIce = Basin_Info$rel_ice)



  runResults_op <- FUN_MOD(InputsModel = inputsModel,
                                   RunOptions  = runOptions,
                                   Param       = param)
  
  plot(runResults_op, Qobs = basinObsTS$Qmm[indRun])


  save(runResults_op, file = file.path(dir_Output,"runResults_op.RData"))
  print(paste0("Saved the result condition at: ",end, " for the Code: ", Code))
  
}



