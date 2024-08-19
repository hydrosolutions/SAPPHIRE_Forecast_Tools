

plot_forecast <- function(forecast_date, 
                          ResPF, 
                          basinObs, 
                          forecast_statistics, 
                          basin_name, 
                          window = 30) {
  
  
  forecast_date <- as.Date(forecast_date)
  # Plotting the forecast  
  Vis_date <- forecast_date - days(window)
  # PF results
  QsimEns <- ResPF$QsimEns
  DatesR <- ResPF$DatesR
  
  ResPF_Q <- QsimEns %>%
    as_tibble() %>%
    mutate(date = as.Date(DatesR))
  
  # Compute mean and standard deviation of Q by Date
  ResPF_initModel <- ResPF_Q %>%
    pivot_longer(cols = -date, names_to = "member", values_to = "Q") %>%
    group_by(date) %>%
    summarise(Q_mean = mean(Q, na.rm = TRUE),  
              Q_median = median(Q, na.rm = TRUE),
              Q_sd = sd(Q, na.rm = TRUE), 
              Q05 = quantile(Q, 0.05, na.rm = TRUE),
              Q95 = quantile(Q, 0.95, na.rm = TRUE))      
  
  
  # Filter the date for visulaization 
  ResPF_initModel_plot <- ResPF_initModel %>%
    filter(date >= Vis_date)
  
  basinObs_plot <- basinObs %>%
    filter(date >= Vis_date)
  
  
  # plot 
  scale_factor <- max(basinObs_plot$Qmm, na.rm = TRUE) / max(basinObs_plot$Ptot, na.rm = TRUE)
  ResPF_initModel_plot$date <- as.Date(ResPF_initModel_plot$date)
  basinObs_plot <- basinObs_plot %>%
    mutate(date = as.Date(date))
  
  
  
  overview <- ggplot() +
    # Precipitation 
    geom_bar(data = basinObs_plot, aes(x = date, y = Ptot * scale_factor, fill = "Precipitation"), stat = "identity", alpha = 0.4) +
    
    # Observations 
    geom_point(data = basinObs_plot, aes(x = date, y = Qmm, color = "Observations"), size = 2) +
    geom_line(data = basinObs_plot, aes(x = date, y = Qmm, color = "Observations"), linewidth = 1) +
    # Model 
    geom_line(data = ResPF_initModel_plot, aes(x = date, y = Q_mean, color = 'Model'), linewidth = 1) +
    geom_line(data = forecast_statistics, aes(x = date, y = Q50, color = 'Median Forecast'), linewidth = 1) +
    geom_ribbon(data = forecast_statistics, aes(x = date, ymin = Q5, ymax = Q95, fill = '90% Quantile'), alpha = 0.5) +
    geom_ribbon(data = forecast_statistics, aes(x = date, ymin = Q25, ymax = Q75, fill = '50% Quantile'), alpha = 0.5) +
    
    labs(title = paste0('Forecast: ', forecast_date," ", basin_name), 
         x = 'Date', 
         y = 'Q (mm/day)', 
         color = "Legend", 
         fill = "Legend") +
    
    # Scales and date formatting
    scale_x_date(date_labels = "%b %d", date_breaks = "5 days", minor_breaks = NULL) +
    scale_y_continuous(name = "Q [mm/d]",
                       sec.axis = sec_axis(~./scale_factor, name = "P [mm/d]")) +
    
    # Theme
    theme_minimal() +
    theme(text = element_text(size = 12), 
          axis.text.x = element_text(size = 10),  
          axis.text.y = element_text(size = 10),  
          axis.title.x = element_text(size = 12), 
          axis.title.y = element_text(size = 12), 
          legend.title = element_text(size = 14), 
          legend.text = element_text(size = 12)) +
    #add frame to the plot
    theme(panel.border = element_rect(colour = "black", fill = NA, linewidth = 1)) +
    
    # Manual scales for colors and fills
    scale_color_manual(values = c("Observations" = 'black', "Median Forecast" = 'red', "Model" = "blue")) +
    scale_fill_manual(values = c("Precipitation" = '#49D8E6', "90% Quantile" = '#F8766D',  "50% Quantile" = '#f6483c'))
  return(overview)
  
}


# function to plot the ensemble forecast for precipitation and temperatiure
# Input:
# start_date: date to start plotting from
# Parameter: Ptot or Temp 
# basinObs: data frame with the control forecast
# basinObs_pf: data frame with the perturbed forecast (as list)
# Output:
# ggplot object
plot_ensemble_forecast <- function(start_date, Parameter, basinObs, basinObs_pf) {
  
  # Filter the control forecast based on the start_date
  basinObs_plot <- basinObs %>%
    filter(date >= start_date)
  
  # Combine the list of perturbed forecasts into one data frame
  basinObs_pf_all <- bind_rows(basinObs_pf, .id = "ID")
  basinObs_pf_all$date <- as.Date(basinObs_pf_all$date)
  basinObs_plot$date <- as.Date(basinObs_plot$date)
  
  # Conditional plotting based on the Parameter input
  if (Parameter == "Temp") {
    plot <- ggplot() +
      geom_line(data = basinObs_pf_all, aes(x = date, y = Temp, group = ID, color = ID)) +
      geom_line(data = basinObs_plot, aes(x = date, y = Temp), color = "black", size = 1.5) +
      labs(title = "Temperature Over Time", x = "Date", y = "Temperature (°C)") +
      theme_minimal() +
      scale_color_viridis_d(name = "Ensemble Member") +
      theme(legend.position = "right")
    
  } else if (Parameter == "Ptot") {
    plot <- ggplot() +
      geom_line(data = basinObs_pf_all, aes(x = date, y = Ptot, group = ID, color = ID)) +
      geom_line(data = basinObs_plot, aes(x = date, y = Ptot), color = "black", size = 1.5) +
      labs(title = "Precipitation Over Time", x = "Date", y = "Precipitation (mm)") +
      scale_x_date(date_labels = "%b %d", date_breaks = "2 days") +
      theme_minimal() +
      theme(legend.position = "right") +
      scale_color_viridis_d(name = "Ensemble Member") +
      theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))
  } else {
    stop("Parameter must be 'Temp' or 'Ptot'")
  }
  
  return(plot)
}





