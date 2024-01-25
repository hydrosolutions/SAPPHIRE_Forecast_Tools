:: Stop the container if it is currently running. 
:: Continue with the execution of the script even if the command fails
docker stop backend || echo.
:: Start backend again to run todays forecast
docker start backend
