:: Stop the container if it is currently running.
:: Continue with the execution of the script even if the command fails
docker stop fcbackend || echo.
:: Wait for 5 seconds
timeout /nobreak /t 5 >nul
:: Change the last successful run date to prepare to rerun the latest forecast.
docker start rerun_backend
:: Start backend again to run todays forecast
docker start fcbackend
