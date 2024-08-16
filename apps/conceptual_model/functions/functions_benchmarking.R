







# gets the pentade in a year for a given date
get_pentad <- function(date){
  day <- day(date)
  month <- month(date)
  year <- year(date)
  month_last <- last_day_of_month(year,month) |> day()
  
  pentad_start <- c(1,6,11,16,21,26)
  pentad_end <- c(5,10,15,20,25,month_last)
  pentad <- seq(1,6, by =1)
  
  pentad_value <- NA
  for(i in pentad){
    if(day >= pentad_start[i] & day <= pentad_end[i]){
      pentad_value <- i+((month-1)*6)
      break
    }
  }
  return(pentad_value)
} 


# gets the last day of the month in a year
last_day_of_month <- function(year, month) {
  Date <- as.Date(paste(year, month, "05", sep="-"))
  last_day <- ceiling_date(Date, "month") - days(1)
  return(last_day)
}

# input: forecast date = pentad start - 1d
get_forecast_end <- function(forecast_date){
  pentad_start <- forecast_date + 1
  # need end_forecast
  # if forecastdate day is 31 then it is 05 
  if (day(forecast_date) == 25){
    pentad_end <- last_day_of_month_date(forecast_date)
  } else {
    pentad_end <- forecast_date + 5
  }
  return(pentad_end)
}




last_day_of_month_date <- function(Date) {
  last_day <- ceiling_date(Date, "month") - days(1)
  return(last_day)
}

# gets the forecasting date from a input period 
pentadal_days <- function(start_date, end_date) {
  # Convert input dates to Date class
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  
  # Initialize an empty character vector to store pentadal days
  pentadal_dates <- c()
  
  current_date <- start_date
  while (current_date <= end_date) {
    day <- day(current_date)
    if (day %in% c(5, 10, 15, 20, 25) || current_date == last_day_of_month_date(current_date)) {
      pentadal_dates <- c(pentadal_dates, as.character(current_date))
    }
    current_date <- current_date + days(1)
  }
  
  return(as.Date(pentadal_dates))
}


################ EVALUATION HYDROMET ####################


# Input: Needs either pentad (numeric 1:72) or forecast_date, Qobs_pentad, Qsim_pentad, factor
# If pentad is provided also needs flag (then comes from get_pentadal_forecast_hydromet)
get_accuracy_hyromet <- function(pentadal_forecast_hydromet){
  
  # if pentadal_forecast_hydromet the column pentad exists: 
  if (("pentad" %in% colnames(pentadal_forecast_hydromet))) {
    result <- pentadal_forecast_hydromet %>%
      group_by(pentad) %>%
      summarize(count_flag_1 = sum(flag == 1, na.rm = TRUE), # Count of flag=1 for each pentad
                # total_entries = n(), # Total number of entries for each pentad
                total_entries = sum(!is.na(flag)), # Total number of non-NA entries for each pentad
                percentage_flag_1 = (count_flag_1 / total_entries)) # Percentage of flag=1 for each pentad
    
    
  } else {
    data <- pentadal_forecast_hydromet
    data$pentad <- NA
    for (i in 1: length(data$forecast_date)) {
      data$pentad[i] <- get_pentad(as.Date(data$forecast_date[i])+1)
    }
    data$flag <- ifelse((data$Qsim_pentad<= (data$Qobs_pentad + data$factor)) & (data$Qsim_pentad >= (data$Qobs_pentad - data$factor)), 1, 0)
    
    result <- data %>%
      group_by(pentad) %>%
      summarize(count_flag_1 = sum(flag == 1, na.rm = TRUE), # Count of flag=1 for each pentad
                # total_entries = n(), # Total number of entries for each pentad
                total_entries = sum(!is.na(flag)), # Total number of non-NA entries for each pentad
                percentage_flag_1 = (count_flag_1 / total_entries)) # Percentage of flag=1 for each pentad
    
    
    
  }
  result$pentad_dates <- pentadal_forecast_hydromet$forecast_date[1:72] + 1
  
  return(result)
}



# Input datarame with Date and 
# Output: Qsim_pentad, forecast_date, pentad_start, pentad
# function needed last_day_of_month, get_pentad
get_pentadal_forecast_hydromet <- function(Q_evaluation, factor) {
  # Ensure Date is in Date format
  Q_evaluation$Date <- as.Date(Q_evaluation$Date)
  
  
  
  start_year <- year(min(Q_evaluation$Date))
  end_year <- year(max(Q_evaluation$Date))
  
  results <- list()
  
  for (year in start_year:end_year) {
    for (month in 1:12) {
      # Calculate the start and end dates for each pentad in the month
      forecast_dates <- c(
        last_day_of_month(ifelse(month == 1, year - 1, year), ifelse(month == 1, 12, month - 1)),
        seq(as.Date(paste(year, month, "05", sep="-")), by = "5 days", length.out = 5)
      )
      
      pentadal_ranges <- c(forecast_dates, last_day_of_month(year, month))
      
      for (i in 1:length(forecast_dates)) {
        pentadal_range_one <- c(forecast_dates[i] + 1, pentadal_ranges[i + 1])
        
        temp_result <- Q_evaluation %>%
          filter(Date >= pentadal_range_one[1] & Date <= pentadal_range_one[2]) %>%
          summarise(
            Qsim_pentad = mean(Qsim, na.rm = TRUE),
            forecast_date = forecast_dates[i],
            pentad_start = pentadal_range_one[1],
            pentad = get_pentad(pentadal_range_one[1])
          )
        
        results[[length(results) + 1]] <- temp_result
      }
    }
  }
  
  # Combine all the results into a single dataframe
  Q_evaluation_pentad <- bind_rows(results)
  
  min_date <- min(factor$forecast_date)
  max_date <- max(factor$forecast_date)
  
  Q_evaluation_pentad <- Q_evaluation_pentad |>
    filter(forecast_date >= min_date & forecast_date <= max_date)
  # Evaluation criteria hydromet
  result <- Q_evaluation_pentad |>
    left_join(factor, by = "forecast_date")
  
  result$flag <- factor(ifelse((result$Qsim_pentad<= (result$Qobs_pentad + result$factor)) & (result$Qsim_pentad >= (result$Qobs_pentad - result$factor)), 1, 0))
  
  return(result)
}
