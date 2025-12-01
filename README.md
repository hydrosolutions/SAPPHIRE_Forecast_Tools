![test and deploy](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/test_deploy_main.yml/badge.svg)
![status: active development](https://img.shields.io/badge/status-active%20development-yellow)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue.svg?logo=docker)

# 💎 SAPPHIRE Forecast Tools

**Open-source operational runoff forecasting — from standalone deployment to system integration**

A modular toolkit for operational hydrological forecasting that works at two levels:

- **Full operational system** — A complete forecasting platform with dashboards, automated pipelines, and bulletin generation, designed for hydromets of countries of the former Soviet Union (pentadal/decadal forecasts, Russian language support)

- **Standalone forecast models** — A backend module where different runoff models can be coupled, runnable independently of the full system

## Key Features

- **Multiple forecast models** — Linear regression (period-wise aggregated auto-regressive), deep learning models (TIDE, TSMixer, TFT) for short-term forecasting, and airGR model suite with added glacier melt functionality
- **Flexible data sources** — Optimized to link with [iEasyHydro High Frequency](https://hf.ieasyhydro.org) but also operational as standalone
- **Forecast dashboard** — Interactive web interface for forecast analysis and production of forecast bulletins
- **Smart workflow orchestration** — Luigi-based pipeline management for automated, scheduled forecasts
- **Tested deployments** — Validated on AWS cloud servers and Ubuntu local server deployments
- **Easy updates & deployment** — Docker-containerized with GitHub Actions for continuous deployment
- **Ensemble forecasting** — Automatically combines models for robust predictions

## Project Origins

Co-designed with the Kyrgyz Hydrometeorological Services as part of the [SAPPHIRE Central Asia project](https://www.hydrosolutions.ch/projects/sapphire-central-asia), funded by the [Swiss Agency for Development and Cooperation](https://www.eda.admin.ch/eda/en/home/fdfa/organisation-fdfa/directorates-divisions/sdc.html). While the full operational workflow is tailored for hydromets of countries of the former Soviet Union, the standalone forecast models are designed for worldwide applicability.

---

# Overview
4 tools are currently deployed via Docker and provided in this repository (see folder apps):
  - A dashboard to configure the forecast tools (currently only available in Russian language)
  - A tool to produce forecasts (in the present version linear regressions as currently employed by the Kyrgyz Hydrometeorological Services)
  - A tool for manual re-run of the latest forecast
  - A dashboard to visualize and download the forecasts

## Available forecast models
The following models are available in the forecast tools:
  - Linear regression models
  - Machine learning models (e.g. Temporal Fusion Transformer)
  - Conceptual rainfall-runoff models (e.g. GR4J)

The basic principles of the hydrological models available in the SAPPHIRE Forecast Tools are described here below. For more technical details, please refer to the respective documentation of the models.

<details>

<summary>English</summary>

**Empirical models**
Empirical models are based on statistical relationships that link the runoff to be predicted with data that is correlated with it, such as recent runoff, snow conditions, rainfall, or temperature.

***Linear regression***
The linear regression (LR) method currently implemented is only considering past runoff and no other predictors for runoff. It is the only method that does not consider weather forecasts for forecasting. It is also the only method which produces pentadal average runoff one day before the start of each pentad and not daily forecasts.

***Autoregressive model***
The auto-regressive integrated moving average (ARIMA) model is often used in the literature to forecast time series data with trend and seasonality.

***Machine learning models***
Machine learning (ML) models are empirical models which can handle many predictors and non-linearity as well as complex relationships that cannot easily be captured by other model types. The machine learning models learn common patterns applicable to all rivers as opposed to tailoring models to individual rivers. We therefore have one model of each model type that is applied to make forecasts for all mountainous rivers.
ML models include static features (elevation, land use, etc.) into the model as predictors.
The currently available machine learning models are suited for forecasting time series with complex patterns. The average forecast of all 3 machine learning models is called the Neural Ensemble (NE).

*Temporal Fusion Transformer (TFT)*
TFT models learn which parts of the entire time series are most important predictors to forecast patterns in the runoff. They are especially good at identifying both short-term and long-term dependencies dynamically (a mechanism called attention) which might be missed by simpler methods as TIDE and are therefore suited for handling slow and/or complex processes. The TFT is the most sophisticated but also the most complex and resource intensive ML model.

*Time-Series Dense Encoder (TIDE)*
TIDE models, like TFT models, learn which parts of the time series are most important but then simplify the time series for forecasting (a mechanism called dense encoding leading to a simplification of the attention mechanism). It is faster and easier to train than TFT but may not capture very slow or complex relationships between the data.

*Time-series Mixer (TSMIXER)*
While TFT and TIDE use static data as a separate layer next to the time-series data, TSMIXER combines both data types and different time steps across time steps and evaluates the impact of each static feature on the dynamic feature. This process is called mixing and allows the model to capture the influence of static features in a more integrated way than TFT and TIDE. TSMIXER is simpler and therefore easier to train than TFT and TIDE, but it does not have the attention mechanism.

**Conceptual models**
Conceptual rainfall runoff models (RRM) implement the main runoff formation processes in a spatially semi-distributed way. Semi-distributed means that we define zones with similar runoff formation characteristics and accumulate runoff from these zones at the outlet. In the Forecast Tools, we correct the simulated forecasts using measured runoff data in a process called Data Assimilation and therefore call the model Rainfall Runoff Assimilation Model (RRAM). Our models uncertainties in model parameters and forcing into account. The forecast range is determined by the parameter and the forcing uncertainty.
This type of forecasting is state-of-the-art and often used in operational runoff forecasting in Central Europe. We have this model type for Ala Archa and Toktogul Inflow only because model setup, calibration and validation are much more involved than for empirical models.

**Ensemble Mean (EM)**
The average pentadal or decadal forecast over all models which have a forecast accuracy of 80% or higher is combined in the ensemble mean.
</details>

<details>

<summary>Русский</summary>

**Эмпирические модели**
Эмпирические модели основаны на статистических взаимосвязях, которые связывают прогнозируемый сток с данными, с ним коррелирующими, такими как недавний сток, снег, осадки или температура.

***Линейная регрессия***
Метод линейной регрессии (LR), реализованный в данный момент, учитывает только прошлый сток и не использует другие предикторы для стока. Это единственный метод, который не использует прогнозы погоды для предсказания. Также это единственный метод, который производит пентадальные средние значения стока за день до начала каждого пентада, а не ежедневные прогнозы.

***Автопрогнозирующая модель***
Модель авторегрессионного интегрированного скользящего среднего (ARIMA) часто используется в литературе для прогнозирования временных рядов с трендом и сезонностью.

***Модели машинного обучения***
Модели машинного обучения (ML) — это эмпирические модели, которые могут обрабатывать множество предикторов и нелинейности, а также сложные зависимости, которые трудно захватить другими типами моделей. Модели машинного обучения изучают общие закономерности, применимые ко всем рекам, в отличие от создания моделей для отдельных рек. Поэтому для всех горных рек используется по одной модели каждого типа.
Модели машинного обучения включают статические характеристики (высота над уровнем моря, тип подстилающей поверхности и др.) как предикторы.
Доступные в настоящее время модели машинного обучения подходят для прогнозирования временных рядов с комплексными паттернами. Средний прогноз всех 3 моделей машинного обучения называется Нейронным ансамблем (NE).

*Temporal Fusion Transformer (TFT)*
Модели TFT изучают, какие части всего временного ряда являются наиболее важными предсказателями для прогнозирования паттернов стока. Они особенно хороши в выявлении как краткосрочных, так и долгосрочных зависимостей динамически (механизм, называемый вниманием), которые могут быть упущены более простыми методами, такими как TIDE, и поэтому подходят для обработки медленных и/или сложных процессов. TFT является самой сложной, но также и самой ресурсозатратной моделью машинного обучения.

*Time-Series Dense Encoder (TIDE)*
Модели TIDE, как и модели TFT, изучают, какие части временного ряда наиболее важны, но затем упрощают временной ряд для прогнозирования (механизм, называемый плотным кодированием, что приводит к упрощению механизма внимания). Эта модель быстрее и проще в обучении, чем TFT, но может не захватывать очень медленные или сложные зависимости между данными.

*Time-series Mixer (TSMIXER)*
В то время как TFT и TIDE используют статические данные как отдельный слой рядом с данными временного ряда, TSMIXER сочетает оба типа данных и различные временные шаги и оценивает влияние каждого статического признака на динамический признак. Этот процесс называется смешиванием и позволяет модели захватывать влияние статических признаков более интегрированным способом, чем TFT и TIDE. TSMIXER проще и поэтому легче обучать, чем TFT и TIDE, но он не имеет механизма внимания.

**Концептуальные модели**
Концептуальные модели формирования стока (RRM) реализуют основные процессы формирования стока в пространственно полусмещенном виде. Полусмещенное означает, что мы определяем зоны с похожими характеристиками формирования стока и аккумулируем сток из этих зон на выходе. В инструментах прогнозирования мы корректируем смоделированные прогнозы с использованием измеренных данных о стоке в процессе, называемом ассимиляцией данных, и поэтому называем модель Моделью ассимиляции стока (RRAM). Мы учитываем неопределенности в параметрах модели и воздействии. Диапазон прогноза определяется неопределенностью параметров и воздействия.
Этот тип прогнозирования является современным и часто используется в оперативном прогнозировании стока в Центральной Европе. Мы имеем этот тип модели только для Ала-Арчи и притока в Токтогул, так как настройка модели, калибровка и валидация гораздо более сложны, чем для эмпирических моделей.

**Среднее ансамбля (EM)**
Средний пентадальный или декадный прогноз по всем моделям с точностью прогноза 80% и выше комбинируется в ансамблевое среднее.


</details>


## Folder structure
All software components are in the apps directory. They are discussed in more detail in the file doc/deployment.md.
Potentially sensitive data that needs to be provided by a hydromet (for example daily discharge data used for the development of the forecast models) is stored in the data folder. Here we provide publicly available data examples from Switzerland for demonstration.
<details>
<summary>Click to expand the folder structure</summary>
Files that need to be reviewed and potentially edited or replaced for local deployment are highlighted with a #.


- `SAPPHIRE_FORECAST_TOOLS`
  - `apps`: The software components of the SAPPHIRE Forecast Tools.
    - `backend` (being deprecated): The backend of the forecast tools. This is the component that produces the forecasts.
    - `config`: A demo-configuration of the forecast tools.
      - `locale`: Translations for the forecast dashboard. Currently only available in English and Russian language.
      - `.env`: Holds file and folder paths as well as access information to the iEasyHydro Database. This file is read by all forecast tools when deployed using Docker.
      - `.env_develop`: Same as .env but for local development. This file is read by all forecast tools when run locally as local folder structure differs from deployed folder structure.
      - `#config_all_stations_library.json`: Information about all stations that are potentially available for the forecasting tools. This includes station codes, names, and coordinates.
      - `#config_development_restrict_station_selection.json`: A list of stations that are available for the development of the forecast models. This file restricts the stations selected by the forecast configuration dashboard to the stations that are actually available for development.
      - `config_output.json`: Defines what outputs are generated by the forecast tools. This file is written by the forecast configuration dashboard.
      - `config_stations_selection.json`: A list of stations selected for the production of forecasts. This file is written by the forecast configuration dashboard.
    - `configuration_dashboard`: A user interface to configure for which stations forecasts are produced and what outputs are generated. The dashboard is written in R and uses the Shiny framework.
      - `www`: Static files (icon Station.jpg) used by the dashboard.
      - `dockerfile`: Dockerfile to build the docker image for the forecast configuration dashboard.
      - `forecast_configuration.R`: The R script that runs the forecast configuration dashboard.
    - `forecast_dashboard`: A user interface to visualize and download the forecasts. The dashboard is written in python and uses the panel framework.
      - `www`: Static files (icon Pentad.jpg) used by the dashboard.
      - `Dockerfile`: Dockerfile to build the docker image for the forecast dashboard.
      - `forecast_dashboard.py`: The python script that runs the forecast dashboard.
    - `iEasyHydroForecast`: A collection of python functions that are used by the linear regression tool.
    - `internal_data`: Data that is written and used by the forecast tools.
      - `forecasts_pentad.csv`: The forecasts produced by the forecast backend. This file is written by the forecast backend and read by the forecast dashboard.
      - `hydrograph_day.csv`: Daily data used for visualization. This file is written by the forecast configuration dashboard and read by the forecast backend.
      - `hydrograph_pentad.csv`: Pentad data used for visualization. This file is written by the forecast configuration dashboard and read by the forecast backend.
      - `latest_successful_run.txt`: A text file that holds the date of the latest successful run of the forecast backend. This file is written and read by the forecast backend.
    - `reset_forecast_run_date`: A module used to re-run the latest forecast. This is useful if new data becomes available that should be included in the latest forecast.
      - `Dockerfile`: Dockerfile to build the docker image for the reset forecast run date tool.
      - `rerun_forecast.py`: The python script that runs the reset forecast run date tool.
      - `requirements.txt`: List of python packages that need to be installed in the docker image.
    - `preprocessing_gateway`: A module that pre-processes gridded meteorological data for the forecast backend.
      - `Dockerfile`: Dockerfile to build the docker image for the preprocessing gateway.
      - `Quantile_Mapping_OP.py`: To downscale operational data to the basin level.
      - `extend_era5_reanalysis.py`: To fill potential gaps in the operational data with reanalysis data.
      - `get_era5_reanalysis_data.py`: To get ERA5-Land data to produce hindcasts.
      - `requirements.txt`: List of python packages that need to be installed in the docker image.
    - `bat` (being deprecated): Batch files that are used for deployment on Windows.
    - `#data`: Example data to demonstrate how the forecast tools work. The Needs to be replaced with data by the hydromet organization for deployment. The data and file formats are described in more detail in the file doc/user_guide.md.
      - `daily_runoff`: Daily discharge data for the development of the forecast models. The data is stored in Excel files. The paths to these files are configured in the .env file.
      - `GIS`: GIS data for the forecast configuration dashboard. The data is stored in shape files. The paths to these files are configured in the .env file.
      - `reports`: Examples of forecast bulletins produced by the forecast tools. Will be generated automatically if it does not exist.
      - `templates`: Templates for the forecast bulletins. The templates are stored in Excel files. The paths to these files are configured in the .env file.
        - `pentad_forecast_bulletin_template.xlsx`: Template for the pentad forecast bulletin. Edit according to your reporting requirements.
    - `doc`: Documentation of the forecast tools.

</details>

# Configuration
The SAPPHIRE Forecast Tools interact with each other through a number of files. The configuration of these files and paths is described in detail in the file [doc/configuration.md](doc/configuration.md).

# Deployment
The SAPPHIRE Forecast Tools are deployed using the Docker system. The deployment of the forecast tools is described in detail in the file [doc/deployment.md](doc/deployment.md).

# Development
If you wish to run the forecast tools individually and locally for development purposes, you can do so by following the instructions in the file [doc/development.md](doc/development.md).

# User guide
Once the forecast tools are deployed with the appropriate **input data**, the user can configure the forecast tools and visualize and download the forecasts using the forecast configuration dashboard and the forecast dashboard. The user guide is available in English language in the file [doc/user_guide.md](doc/user_guide.md).

# Data requirements
The forecast tools rely on the availability of daily average discharge data. The data can be read from the iEasyHydro database or from local excel files. We include publicly available daily discharge data from the Swiss Federal Office for the Environment (FOEN) in this repository. You find the original data [here](https://www.hydrodaten.admin.ch/en/seen-und-fluesse).

# Collaboration
Input from users is welcome. Please use the GitHub issue tracker to report bugs or suggest improvements. If you would like to contribute to the code, please fork the repository and submit a pull request. We will review the pull request and merge it if it fits the scope of the project. Please note that this is an open source project released under the MIT license, under which all contributions fall. Contributors are expected to adhere to the [Contributor Covenant code of conduct](https://www.contributor-covenant.org/).

# Funding and contributions
The [SAPPHIRE Central Asia project](https://www.hydrosolutions.ch/projects/sapphire-central-asia) is funded by the [Swiss Agency for Development and Cooperation (SDC)](https://www.sdc-cde.ch/en) and implemented by [hydrosolutions GmbH](https://www.hydrosolutions.ch/) in close collaboration with the Department of Operational Hydrology of Kyrgyz Hydromet. The SAPPHIRE Forecast Tools are developed by [hydrosolutions GmbH](https://www.hydrosolutions.ch/) with support from [encode](http://encode.global).

This is a collaboraion  project where each contributor profited from input of all other contributors. However, we'd like to acknowledge the major code contributions here more specifically:

- Aidar Zhumabaev [@LagmanEater](https://github.com/LagmanEater): Implementation of the forecast configuration dashboard (module forecast_configuration).
- Maxat Pernebaev [@maxatp](https://github.com/maxatp): Code review and refactoring of the first version of the forecast tools.
- Adrian Kreiner [@adriankreiner](https://github.com/adriankreiner): Development of the packages [airGR_GM]() and [airgrdatassim](). Implementation of the operational hydrological forecasting with conceptual rainfall-runoff models (module conceptual_model).
- Sandro Hunziker [@sandrohuni](https://github.com/sandrohuni): Implementation of the pre-processing of gridded meteorological data (module preprocessing_gateway) and implementation of the operational hydrological forecasting using machine learning models (module machine_learning).
- Vjekoslav Večković [@vjekoslavveckovic](https://github.com/vjekoslavveckovic): Implementation of custom functions and complex interactivity for the forecast dashboard (module forecast_dashboard).
- Davor Škalec [@skalecdavor](https://github.com/skalecdavor) and Vatroslav Čelan [@vatrocelan](https://github.com/vatrocelan): Support with the integration of the ieasyreports and ieasyhydro-python-sdk libraries.
- Beatrice Marti [@mabesa](https://github.com/mabesa): Design and implementation of the forecast tools and all modules not named above and coordination of the project.
