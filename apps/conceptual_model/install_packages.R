

# Intendent to be run from the ~app/ direction in the Docker container
requirements <- readLines("/conceptual_model/requirements.txt")

# Always needs for install below
if (!requireNamespace("devtools", quietly = TRUE)) {
  install.packages("devtools")
}

# Install CRAN packages
for (pkg in requirements) {
  
  package <- strsplit(pkg, "==")[[1]][1]
  version <- strsplit(pkg, "==")[[1]][2]
  
  if (!requireNamespace(package, quietly = TRUE) || packageVersion(package) != version) {
    print(paste("Installing package", package, "version", version))
    devtools::install_version(package, version = version, repos = "http://cran.us.r-project.org")
  }
}

# Install GitHub packages
devtools::install_github("hydrosolutions/airGR_GM")
devtools::install_github("hydrosolutions/airgrdatassim")

cat("All packages installed successfully.\n")
