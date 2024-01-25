:: Stop the container if it is currently running.
:: Continue with the execution of the script even if the command fails
docker stop backend || echo.
:: Wait for 5 seconds
timeout /nobreak /t 5 >nul
:: Start backend again to run todays forecast
docker start backend
