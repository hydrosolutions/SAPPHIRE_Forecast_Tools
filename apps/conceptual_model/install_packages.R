

# Intendent to be run from the ~app/ direction in the Docker container
requirements <- readLines("apps/conceptual_model/requirements.txt")

# Always needs for install below
if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes", dependencies = TRUE)
}

# Install CRAN packages
for (pkg in requirements) {
  
  package <- strsplit(pkg, "==")[[1]][1]
  version <- strsplit(pkg, "==")[[1]][2]
  
  if (!requireNamespace(package, quietly = TRUE) || packageVersion(package) != version) {
    print(paste("Installing package", package, "version", version))
    remotes::install_version(
      package, 
      version = version, 
      repos = "http://cran.us.r-project.org", 
      dependencies = TRUE)
  }
  
  # Test if the package has been installed suscessfully
  if (!requireNamespace(package, quietly = TRUE)) {
    stop(paste("Package", package, "not installed."))
  }
}

# Install GitHub packages
remotes::install_github("hydrosolutions/airGR_GM")
remotes::install_github("hydrosolutions/airgrdatassim")

cat("All packages installed successfully.\n")
