FROM rocker/tidyverse:4.4.1 AS base

# Rocker image with R and Rpackages devtools and tidyverse pre-installed,
# unfortunately not available for ARM architecture
#FROM rocker/tidyverse:4.4.1 AS base

# Order commands from less-frequently changed to more-frequently changed

# Set environment
ENV SAPPHIRE_OPDEV_ENV=True

# Set working directory
WORKDIR /app

# Copy the directory
COPY apps/conceptual_model /app/apps/conceptual_model

# Install packages
#RUN R -e "install.packages('remotes')"
RUN Rscript apps/conceptual_model/install_packages.R

# Verify that the remotes package is installed
RUN Rscript -e "if (!requireNamespace('remotes', quietly = TRUE)) { stop('remotes not installed') }"

# Verify that the tidyverse package is installed
RUN Rscript -e "if (!requireNamespace('tidyverse', quietly = TRUE)) { stop('tidyverse not installed') }"

# Run the application
CMD ["Rscript", "apps/conceptual_model/run_operation_forecasting_CM.R"]
#CMD ["R", "-e", "print('Hello, World!')"]