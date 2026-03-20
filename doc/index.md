# SAPPHIRE Forecast Tools

**Open-source operational runoff forecasting — from standalone deployment to system integration**

A modular toolkit for operational hydrological forecasting that works at two levels:

- **Full operational system** — A complete forecasting platform with dashboards, automated pipelines, and bulletin generation, designed for hydromets of countries of the former Soviet Union (pentadal/decadal forecasts, Russian language support)

- **Standalone forecast models** — A backend module where different runoff models can be coupled, runnable independently of the full system

## Key Features

- **Multiple forecast models** — Linear regression, deep learning models (TIDE, TSMixer, TFT), and airGR model suite with glacier melt
- **Flexible data sources** — Optimized for iEasyHydro High Frequency but also works standalone
- **Forecast dashboard** — Interactive web interface for forecast analysis and bulletin production
- **Smart workflow orchestration** — Luigi-based pipeline management for automated, scheduled forecasts
- **Tested deployments** — Validated on AWS cloud servers and Ubuntu local server deployments
- **Easy updates & deployment** — Docker-containerized with GitHub Actions for continuous deployment
- **Ensemble forecasting** — Automatically combines models for robust predictions

## Quick Links

<div class="grid cards" markdown>

-   :material-book-open-variant:{ .lg .middle } **User Guide**

    ---

    Learn how to use the forecast tools and dashboards

    [:octicons-arrow-right-24: User Guide](user_guide.md)

-   :material-cog:{ .lg .middle } **Configuration**

    ---

    Configure the forecast tools for your deployment

    [:octicons-arrow-right-24: Configuration](configuration.md)

-   :material-server:{ .lg .middle } **Deployment**

    ---

    Deploy the forecast tools to your server

    [:octicons-arrow-right-24: Deployment](deployment.md)

-   :material-code-braces:{ .lg .middle } **Development**

    ---

    Contribute to the forecast tools development

    [:octicons-arrow-right-24: Development](development.md)

</div>

## Available Forecast Models

| Model Type | Models | Description |
|------------|--------|-------------|
| **Empirical** | Linear Regression | Period-wise aggregated auto-regressive model |
| **Machine Learning** | TFT, TIDE, TSMixer | Deep learning models for complex patterns |
| **Conceptual** | airGR (GR4J) | Rainfall-runoff models with glacier melt |
| **Ensemble** | Neural Ensemble, Ensemble Mean | Combined forecasts for robustness |

## Project Origins

Co-designed with the Kyrgyz Hydrometeorological Services as part of the [SAPPHIRE Central Asia project](https://www.hydrosolutions.ch/projects/sapphire-central-asia), funded by the [Swiss Agency for Development and Cooperation](https://www.eda.admin.ch/eda/en/home/fdfa/organisation-fdfa/directorates-divisions/sdc.html).

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/hydrosolutions/SAPPHIRE_forecast_tools/blob/main/LICENSE) file for details.
