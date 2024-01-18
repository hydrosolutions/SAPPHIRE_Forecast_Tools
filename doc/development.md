# Development

This document describes how to develop the application.

## Prerequisites
The softwar has been developed on a Mac computer and packaged with Ubuntu base images using Docker using GitHub Actions (with workflow instructions in .github/workflows/main.yml). Albeit not strictly necessary for the further development of the software, we suggest that you install Docker on your computer so you can test dockerization of sofware components locally. The Docker installation instructions can be found [here](https://docs.docker.com/install/).

### Configuration dashboard
The forecast configuration dashboard is written in R and uses the Shiny framework. To run the dashboard locally, you need to install R. The installation instructions can be found [here](https://rstudio-education.github.io/hopr/starting.html). We recommend RStudio as an IDE for R development. The installation instructions can be found [here](https://posit.co/download/rstudio-desktop/).

You will further need to install the following R packages (you can do so by running the following commands in the R console):
```R
# Data handling libraries
install.packages("readxl")
install.packages("dplyr")
install.packages("jsonlite")
install.packages("sf")
install.packages("here")
## Shiny related libraries
install.packages("shiny")
install.packages("shinydashboard")
install.packages("shinyWidgets")
## Plotting and mapping libraries
install.packages("leaflet")
```


## Dockerization

### Configuration dashboard
Note that at the time of writing, a Docker base image with R and RShiny is not available for the ARM architecture (the latest Mac processors). The configuration dashboard has, with the current setup, been dockerized in Ubuntu.

The forecast dashboard is dockerized using the Dockerfile in the apps/configuration_dashboard folder. To build the docker image locally, run the following command in the root directory of the repository:
```bash
docker build --no-cache -t station_dashboard -f ./apps/configuration_dashboard/dockerfile .
```
Run the image locally for testing (not for deployment). Replace <full_path_to> with your local path to the folders.
```bash
docker run -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/config:/app/apps/config -v <full_path_to>/data:/app/data -p 3647:3647 --name station_dashboard
```






