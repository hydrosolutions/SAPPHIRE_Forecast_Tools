library(here)
library(jsonlite)


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

print(Sys.getenv("ieasyhydroforecast_PATH_TO_BASININFO"))
 
##################################################################
# Have to be in folder: cd /Users/adrian/Documents/GitHub/SAPPHIRE_Forecast_Tools/apps/conceptual_model
# run with: SAPPHIRE_OPDEV_ENV=True Rscript run_operation_forecasting_CM.R
# env file is in: /data/sensitive_data_forecast_tools/config/.env_develop_kghm
# .json file is in: 
##################################################################
# 0 Library and Function ####
library(devtools, quietly = TRUE)
# devtools::install_github("hydrosolutions/airGR_GM")
# devtools::install_github("hydrosolutions/airgrdatassim")
library(airGR, quietly = TRUE)
library(airGRdatassim, quietly = TRUE)
library(tidyverse, quietly = TRUE)
library(usethis, quietly = TRUE)


source("functions/functions_operational.R")
source("functions/functions_plot.R")
source("functions/functions_hindcast.R")
################### CHANGE ###################
Code <- 15194
forecast_date <- as.Date("2024-07-31") # change this when DG is running again to today()
################### INITIALIZE ###################


## 0.1 Function ####
# general path
dir_Q <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_Q"))
dir_Control <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_CF"))
dir_Ensemble <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_PF"))
dir_hindcast <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_HIND"))

fun_mod_mapping <- list(
  `15194` = RunModel_CemaNeigeGR4J_Glacier,
  `16936` = RunModel_CemaNeigeGR6J
  # Add more code-function pairs here
)


FUN_MOD <- fun_mod_mapping[[as.character(Code)]]

# # basin specific path
dir_basin <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_BASININFO"), Code)
dir_Output <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_INITCOND"), Code)
dir_Results <- file.path(Sys.getenv("ieasyhydroforecast_PATH_TO_RESULT"), Code)



# Step 1: Prepare Input data ####
## 1.1 Initial ####

forecast_date_format <- forecast_date %>%
  format(format = "%Y%m%d")

load(file.path(dir_basin,"param.RData"))
load(file.path(dir_basin,"Basin_Info.RData"))


Nb_ens <- c(1:50)
NbMbr <- 2L
DaMethod <- "PF"
StatePert <- c("Rout", "Prod", "UH1", "UH2")
eps <- 0.65

lag_days <- 10
forecast_mode <- "daily"
################### RUNNING ###################

# file names for control memnber (cf) and ensemble member (pf) forecast and for the hindcasting forcing data (starts from 2009-01-01)
# cf: all basins in one file
cfP_forecast_filename <- "00003_P_control_member.csv"
cfT_forecast_filename <- "00003_T_control_member.csv"

# pf: one file per basin
pfP_forecast_filename <- paste0(Code,"_P_ensemble_forecast.csv")
pfT_forecast_filename <- paste0(Code,"_T_ensemble_forecast.csv")

# hindcast filename
P_hindcast_filename <- "00003_P_reanalysis.csv"
T_hindcast_filename <- "00003_T_reanalysis.csv"



## 1.2 Discharge observations ####
Q_obs <- process_discharge_data(file.path(dir_Q, "runoff_day.csv"), Basin_Info$BasinCode, Basin_Info$BasinArea_m2)

# 1.3 Operational forcing data  ####
basinObs_cf <- process_forecast_forcing(member_id = "cf",
                                        Basin_code = Basin_Info$BasinCode,
                                        file_path_Ptot = file.path(dir_Control, cfP_forecast_filename),
                                        file_path_Temp = file.path(dir_Control, cfT_forecast_filename),
                                        Lat = Basin_Info$BasinLat_rad,
                                        Q_obs = Q_obs)


basinObs_pf <- process_forecast_forcing(member_id = Nb_ens,
                                        Basin_code = Basin_Info$BasinCode,
                                        file_path_Ptot = file.path(dir_Ensemble, pfP_forecast_filename),
                                        file_path_Temp = file.path(dir_Ensemble, pfT_forecast_filename),
                                        Lat = Basin_Info$BasinLat_rad)

inputsModel_cf <- airGR::CreateInputsModel(FUN_MOD   = FUN_MOD,
                                           DatesR    = basinObs_cf$date,
                                           Precip    = basinObs_cf$Ptot,
                                           PotEvap   = basinObs_cf$PET,
                                           TempMean  = basinObs_cf$Temp,
                                           HypsoData = Basin_Info$HypsoData,
                                           ZInputs   = median(Basin_Info$HypsoData),
                                           verbose = FALSE,
                                           GradT = Basin_Info$GradT,
                                           GradP = Basin_Info$k_value)

# plotting
plot_ensemble_forecast( start_date = as.Date(forecast_date-15) , Parameter = "Ptot", basinObs_cf, basinObs_pf)
plot_ensemble_forecast( start_date = as.Date(forecast_date-15) , Parameter = "Temp", basinObs_cf, basinObs_pf)


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

# Step 6: Plotting  ####
plot <- plot_forecast(forecast_date = forecast_date,
                      ResPF = ResPF,
                      basinObs = basinObs_cf,
                      forecast_statistics,
                      basin_name = Basin_Info$BasinName,
                      window = 30)

plot
ggsave(plot,file = paste0(dir_Results,"/plot/Overviewplot_",Basin_Info$BasinCode,"_",forecast_date_format,".pdf"), width = 10, height = 6, dpi = 300)

# Step 7: Hindcast and save  ####
forecast_statistics <- forecast_statistics %>%
  mutate(forecast_date = forecast_date) %>%
  select(forecast_date, everything())

existing_forecasts <- try(read_csv(paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), show_col_types = FALSE), silent = TRUE)

if (inherits(existing_forecasts, "try-error") || all(is.na(existing_forecasts)))  {
  print("Empty file or file does not exist")
  # Save the new forecast statistics
  write.csv(forecast_statistics, paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)
  last_forecast_date <- forecast_date
} else {
  existing_forecasts <- existing_forecasts %>%
    mutate(forecast_date = as.Date(forecast_date, format = "%d.%m.%Y"),
           date = as.Date(date, format = "%d.%m.%Y"))
  last_forecast_date <- as.Date(max(existing_forecasts$forecast_date))
}
#

# MULTIPLE run a day
if ((forecast_mode == "daily") & (forecast_date - last_forecast_date == 0)){
  print("Already saved")
  # stop the script
} else {

  # HINDCASTING IF THERE ARE MISSING FORECAST (daily)
  if ((forecast_mode == "daily") & (forecast_date - last_forecast_date > 1)) {
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

    new_forecast <- rbind(hindcast, forecast_statistics)
    total_forecast <- rbind(existing_forecasts, new_forecast)
    write.csv(total_forecast, paste0(dir_Results, "/data/daily_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)

  } else {
    print("no hindcasting necessary")
    new_forecast <- forecast_statistics
  }


  # Step 8: Pentadal   ####
  # read the pentadal data
  existing_pentad <- try(read_csv(paste0(dir_Results, "/data/pentad_", Basin_Info$BasinCode, ".csv"), show_col_types = FALSE), silent = TRUE)
  existing_pentad <- existing_pentad %>%
    mutate(forecast_date = as.Date(forecast_date, format = "%d.%m.%Y"))

  pentadal_steps <- pentadal_days(start_date_hindcast, forecast_date)

  new_pentad <- convert_daily_to_pentad_decad(new_forecast, pentadal_steps)

  total_pentad <- rbind(existing_pentad, new_pentad)
  write.csv(total_pentad, paste0(dir_Results, "/data/pentad_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)

  # Step 9: Decadal    ####
  existing_decad <- try(read_csv(paste0(dir_Results, "/data/decad_", Basin_Info$BasinCode, ".csv"), show_col_types = FALSE), silent = TRUE)
  existing_decad <- existing_decad %>%
    mutate(forecast_date = as.Date(forecast_date, format = "%d.%m.%Y"))

  decadal_steps <- decadal_days(start_date_hindcast, forecast_date)
  new_decad <- convert_daily_to_pentad_decad(new_forecast, decadal_steps)


  total_decad <- rbind(existing_decad, new_decad)
  write.csv(total_decad, paste0(dir_Results, "/data/decad_", Basin_Info$BasinCode, ".csv"), row.names = FALSE)
}




