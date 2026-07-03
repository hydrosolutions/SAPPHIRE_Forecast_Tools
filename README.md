![test and deploy](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools/actions/workflows/test_deploy_main.yml/badge.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue.svg?logo=docker)

# 💎 SAPPHIRE Forecast Tools

**Open-source operational runoff forecasting — from standalone deployment to system integration**

A modular toolkit for operational hydrological forecasting that works at two levels:

- **Full operational system** — A complete forecasting platform with dashboards, automated pipelines, and bulletin generation, designed for hydromets of countries of the former Soviet Union (pentadal/decadal forecasts, Russian language support)

- **Standalone forecast models** — A backend module where different runoff models can be coupled, runnable independently of the full system

## Maintenance Status

🟢 **Active** – Developed & maintained by [hydrosolutions](https://github.com/hydrosolutions)


## Key Features

- **Multiple forecast models** — Linear regression (temporally aggregated auto-regressive model with a direct forecasting framework), deep learning models (TIDE, TSMixer, TFT) for short-term forecasting, and airGR model suite with added glacier melt functionality
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
TFT models learn which parts of the input are the most important predictors for forecasting patterns in the runoff. They combine Long-Short Term Memory (LSTM) layers, which track recent and sequential changes in the series, with a self-attention mechanism, which weighs how strongly each past moment relates to the forecast. This lets the model dynamically focus on different parts of the input depending on the conditions, rather than treating every time step as equally important, and makes it well suited to capturing both short- and long-term dependencies as well as complex relationships in the data. The TFT is the most sophisticated of these models, but also the most complex and resource-intensive to train.
(Lim, B., Arık, S. Ö., Loeff, N., & Pfister, T. (2021). Temporal Fusion Transformers for interpretable multi-horizon time series forecasting. International Journal of Forecasting, 37(4), 1748–1764. https://doi.org/10.1016/j.ijforecast.2021.03.012 (arXiv:1912.09363))


*Time-Series Dense Encoder (TIDE)*
TIDE models forecast by passing the input through a series of dense layers, rather than using an attention mechanism like the TFT. A global skip connection carries the input forward to retain the simple, linear relationships in the data, while the dense layers add non-linear components when needed. The result is a model with a simpler architecture that is less resource-intensive to train and to run predictions with than the TFT, while remaining well suited for forecasting. 
(Das, A., Kong, W., Leach, A., Mathur, S., Sen, R., & Yu, R. (2023). Long-term forecasting with TiDE: Time-series Dense Encoder. Transactions on Machine Learning Research. arXiv:2304.08424)


*Time-series Mixer (TSMIXER)*
TSMIXER models forecast by mixing information across the time and predictor dimensions, combining how values evolve over time with how the different predictors interact with one another. It is based entirely on dense layers, like the TIDE model, which makes it faster and less resource-intensive than the TFT, and comparable to the TIDE model.
(Chen, S.-A., Li, C.-L., Yoder, N., Arık, S. Ö., & Pfister, T. (2023). TSMixer: An all-MLP architecture for time series forecasting. Transactions on Machine Learning Research. arXiv:2303.06053)


**Conceptual models**
Conceptual rainfall runoff models (RRM) implement the main runoff formation processes in a spatially semi-distributed way. Semi-distributed means that we define zones with similar runoff formation characteristics and accumulate runoff from these zones at the outlet. In the Forecast Tools, we correct the simulated forecasts using measured runoff data in a process called Data Assimilation and therefore call the model Rainfall Runoff Assimilation Model (RRAM). Our models uncertainties in model parameters and forcing into account. The forecast range is determined by the parameter and the forcing uncertainty.
This type of forecasting is state-of-the-art and often used in operational runoff forecasting in Central Europe. We have this model type for Ala Archa and Toktogul Inflow only because model setup, calibration and validation are much more involved than for empirical models.

**Ensemble Mean (EM)**
The average pentadal or decadal forecast over all models which have a forecast accuracy of 80% or higher is combined in the ensemble mean.

**Long-Term Forecast Models**

The long-term forecasting module contains three types of models. This is a quick overview; a more detailed explanation of the different input-variable setups can be found in the [readme.md](apps/long_term_forecasting/readme.md) of the Long-Term Forecasting Module.

*Linear Regression*
(Bayesian) linear regressions take multiple features that are highly correlated with the forecast period. Predictors are selected automatically based on correlation, and the models are fitted separately per gauging station and time of year. By making the regression Bayesian, we also obtain a probabilistic forecast, which gives us a measure of uncertainty. Linear regressions are used for monthly, quarterly, and seasonal predictions.

*Gradient Boosted Trees*
Gradient Boosted Trees (GBT) build many regression trees that iteratively correct each other's errors, making the approach powerful. They rely on handcrafted features, and the models are trained jointly across all basins. Three different GBT implementations are used: XGBoost, LightGBM, and CatBoost. GBT models are used for monthly predictions only.

*Uncertainty Network (MC ALD)*
To produce a probabilistic output and apply a small bias correction, a compact dense neural network is trained to predict the scale and asymmetry of an Asymmetric Laplace Distribution centered on the ensemble mean (the average prediction across all long-term forecasting models). It uses ensemble statistics (e.g. mean and standard deviation) along with historical error statistics as inputs. This model runs for monthly predictions only.



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
Модели TFT определяют, какие части входных данных являются наиболее важными предикторами для прогнозирования закономерностей в стоке. Они объединяют слои долгой краткосрочной памяти (LSTM), которые отслеживают недавние и последовательные изменения во временном ряду, с механизмом самовнимания (self-attention), который оценивает, насколько сильно каждый прошлый момент связан с прогнозом. Это позволяет модели динамически фокусироваться на разных частях входных данных в зависимости от условий, а не рассматривать каждый временной шаг как одинаково важный, и делает её хорошо приспособленной для выявления как краткосрочных, так и долгосрочных зависимостей, а также сложных взаимосвязей в данных. TFT является наиболее совершенной из этих моделей, но в то же время наиболее сложной и ресурсоёмкой в обучении.

*Time-Series Dense Encoder (TIDE)*
Модели TiDE строят прогноз, пропуская входные данные через серию полносвязных слоёв, а не используя механизм внимания, как TFT. Глобальное пропускающее соединение (skip connection) передаёт входные данные напрямую вперёд, чтобы сохранить простые линейные зависимости в данных, тогда как полносвязные слои добавляют нелинейные компоненты, когда это необходимо. В результате получается модель с более простой архитектурой, менее ресурсоёмкая в обучении и в построении прогнозов, чем TFT, но при этом хорошо подходящая для прогнозирования.

*Time-series Mixer (TSMIXER)*
Модели TSMixer строят прогноз, смешивая информацию по измерениям времени и предикторов, объединяя то, как значения изменяются во времени, с тем, как различные предикторы взаимодействуют друг с другом. Она целиком основана на полносвязных слоях, как и модель TiDE, что делает её быстрее и менее ресурсоёмкой, чем TFT, и сопоставимой с моделью TiDE.

**Концептуальные модели**
Концептуальные модели формирования стока (RRM) реализуют основные процессы формирования стока в пространственно полусмещенном виде. Полусмещенное означает, что мы определяем зоны с похожими характеристиками формирования стока и аккумулируем сток из этих зон на выходе. В инструментах прогнозирования мы корректируем смоделированные прогнозы с использованием измеренных данных о стоке в процессе, называемом ассимиляцией данных, и поэтому называем модель Моделью ассимиляции стока (RRAM). Мы учитываем неопределенности в параметрах модели и воздействии. Диапазон прогноза определяется неопределенностью параметров и воздействия.
Этот тип прогнозирования является современным и часто используется в оперативном прогнозировании стока в Центральной Европе. Мы имеем этот тип модели только для Ала-Арчи и притока в Токтогул, так как настройка модели, калибровка и валидация гораздо более сложны, чем для эмпирических моделей.

**Среднее ансамбля (EM)**
Средний пентадальный или декадный прогноз по всем моделям с точностью прогноза 80% и выше комбинируется в ансамблевое среднее.

**долгосрочное прогнозирование**

Модуль долгосрочного прогнозирования содержит три типа моделей. Это краткий обзор; более подробное описание различных конфигураций входных переменных можно найти в файле [readme.md](apps/long_term_forecasting/readme.md) Модуля долгосрочного прогнозирования.

*(Bayesian) Linear Regression* 
(Байесовские) линейные регрессии используют несколько признаков, которые сильно коррелируют с прогнозируемым периодом. Предикторы отбираются автоматически на основе корреляции, а модели обучаются отдельно для каждого гидрологического поста и времени года. Делая регрессию байесовской, мы также получаем вероятностный прогноз, что даёт нам меру неопределённости. Линейные регрессии используются для месячных, квартальных и сезонных прогнозов.

*Gradient Boosted Trees (GBT)*
Деревья с градиентным бустингом (Gradient Boosted Trees, GBT) строят множество регрессионных деревьев, которые итеративно исправляют ошибки друг друга, что делает этот подход мощным. Они опираются на признаки, сконструированные вручную, а модели обучаются совместно по всем бассейнам. Используются три различных реализации GBT: XGBoost, LightGBM и CatBoost. Модели GBT используются только для месячных прогнозов.

*Uncertainty Network (MC-ALD)*
Чтобы получить вероятностный результат и применить небольшую коррекцию смещения, компактная полносвязная нейронная сеть обучается предсказывать масштаб и асимметрию асимметричного распределения Лапласа, центрированного на среднем по ансамблю (усреднённом прогнозе по всем моделям долгосрочного прогнозирования). В качестве входных данных она использует статистики ансамбля (например, среднее и стандартное отклонение) наряду со статистиками исторических ошибок. Эта модель работает только для месячных прогнозов.


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
      - `pyproject.toml`: Project configuration and dependencies managed by uv.
      - `uv.lock`: Lock file for reproducible dependency resolution.
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

## Code Quality
This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting (configuration in `ruff.toml`). A pre-commit hook enforces this automatically on each commit. To set up:

```bash
pip install pre-commit
pre-commit install
```

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

# Maintenance Status

🟢 **Active Development**

This repository is part of an ongoing project and is actively maintained.
