# Forecast Dashboard

General overview

## Predictor tab

## Forecast tab

### Linear regression card
This card allows the user to select and de-select points used to derive the linear regression model. It shows all available data that was used to create the hindcasts.

# Troubbleshooting

<details>
<summary><b>I have added new data to the `daily_runoff` directory, but I don't see the data in the Linear regression card.</b></summary>
The forecast dashboard reads the data for the linear regression card from the results of the linear regression module in the `intermediate_data` directory in the data folder. If you add older data to the `daily_runoff` directory in the data folder, you therefore need to manually update the hindcasts. Edit the file `linreg_last_successful_run_linreg.txt` file in the `intermediate_data` directory of your data folder and change the date to the first day of your new data. Then either wait for the next regular run of the forecast tools or trigger a manual re-run in the linear regression card.
</details>