FROM r-base:4.1.1 AS base

# Rocker image with R and Rpackages devtools and tidyverse pre-installed,
# unfortunately not available for ARM architecture
#FROM rocker/tidyverse:4.4.1 AS base

# Order commands from less-frequently changed to more-frequently changed
# Install necessary build tools
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev

# Install R packages
RUN Rscript apps/conceptual_model/install_packages.R

# Set working directory
WORKDIR /app

# Copy the directory
COPY apps/conceptual_model /app/apps/conceptual_model

# Run the application
CMD ["R", "-e", "print('Hello, World!')"]