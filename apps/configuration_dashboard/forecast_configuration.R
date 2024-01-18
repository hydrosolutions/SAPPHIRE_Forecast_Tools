## Shiny dashboard for the selection of river runoff stations for forecasts. 

# 0 Libraries

## Core libraries
library(readxl)
library(dplyr)
library(jsonlite)
library(sf)
library(here)
## Shiny related libraries
library(shiny)
library(shinydashboard)
library(shinyWidgets)
## Plotting and mapping libraries
library(leaflet)


# 1 Configuration
# Check if this script is run from within a docker container. 
# We do this by checking if an environment variable set by the Dockerfile is 
# present or not. 
if (Sys.getenv("IN_DOCKER_CONTAINER")=="") {
  # Environment variable IN_DOCKER_CONTAINER is not set. Run from local machine
  # This code assumes that forecast_configuration has been opened in 
  # apps/configuration_dashboard for development
  setwd(here())
  print(getwd())
  setwd("apps/configuration_dashboard")
  readRenviron("../config/.env_develop")
} else { 
  # Environment variable IN_DOCKER_CONTAINER is set. Run from docker container
  setwd(here()) #sometimes setwd() function can be error-prone, restart the R session can help 
  print(getwd())
  readRenviron("config/.env")
}

config_dir = Sys.getenv("ieasyforecast_configuration_path")
config_all_stations_file_name = Sys.getenv("ieasyforecast_config_file_all_stations")
config_station_selection_file_name = Sys.getenv("ieasyforecast_config_file_station_selection") #station selection
config_output_file_name = Sys.getenv("ieasyforecast_config_file_output") #excel write TRUE/FALSE

gis_data_dir = Sys.getenv("ieasyforecast_gis_directory_path")
#gis_station_file_name = Sys.getenv("ieasyforecast_station_coordinates_file_name")
gis_shape_file_name = Sys.getenv("ieasyforecast_country_borders_file_name")


# 2 Data
## Load shapefile
shp_file <- st_read(paste0(gis_data_dir,"/",gis_shape_file_name))

## JSON 
station_library <- fromJSON(paste0(config_dir,"/", config_all_stations_file_name))

### Filter out stations whose IDs start with "3" (in the Central Asian context this 
# means that meteo stations are not displayed) and (option) exclude specific 
# stations (e.g. 9999999999999) from the json of available stations. 
exclude_ids <- c("9999999999999")   
filtered_ids <- names(station_library$stations_available_for_forecast)[sapply(names(station_library$stations_available_for_forecast), function(id) !(grepl("^3", id)) && !(id %in% exclude_ids))]
filtered_station_library <- station_library
filtered_station_library$stations_available_for_forecast <- station_library$stations_available_for_forecast[filtered_ids]
station_library <- filtered_station_library

config_outputs_Q <- fromJSON(paste0(config_dir,"/",config_station_selection_file_name))
excel_config <- jsonlite::fromJSON(paste0(config_dir,"/",config_output_file_name))

### Coordinates from JSON for map
station_data <- station_library$stations_available_for_forecast

stations_df <- do.call(rbind, lapply(names(station_data), function(station_id) {
  cbind(
    id = station_data[[station_id]]$code,
    name_ru = station_data[[station_id]]$name_ru,
    lat = station_data[[station_id]]$lat,
    long = station_data[[station_id]]$long
  )
}))

### Convert to a dataframe and ensure types are correct
stations_df <- as.data.frame(stations_df)
stations_df$lat <- as.numeric(stations_df$lat)
stations_df$long <- as.numeric(stations_df$long)
stations_df$id <- as.factor(stations_df$id)


# UI
ui <- dashboardPage(
  # Define the header of the dashboard
  dashboardHeader(title =   tags$div(
    style = "position: relative; display: flex; justify-content: left; align-items: left;", 
    # Save the jpg in a folder www
    tags$div(tags$img(src='Station.jpg',height='30',width='30'), style = "margin-right:10px;"),
    tags$span("Выбор гидропостов для пентадного прогноза"),
    tags$button(icon("question-circle"), type = "button", 
                class = "btn btn-primary", id = "docButton", 
                style = "position: absolute; right: 10px; top: 50%; transform: translateY(-50%);"))),
  
  # Define the sidebar of the dashboard
  dashboardSidebar(disable = TRUE),
  
  # Define the body of the dashboard
  dashboardBody(
    tags$head(
      tags$script("document.title = 'Выбор гидропостов для пентадного прогноза';",
        HTML("
          $(document).on('click', '#docButton', function(e) {
            e.preventDefault();
            Shiny.setInputValue('show_docs', Math.random());
          });
        ")
      ),
      
      tags$style(type="text/css", "
        body, .skin-blue .wrapper, .content-wrapper, .main-content {
        background-color: #f1f1f1;
        }
        .js-plotly-plot .plot-container .svg-container {
          overflow: hidden !important;
        }
        .shiny-input-container input[value='NA'] {
          color: transparent;
          caret-color: black;
        }
  
      /* Adjusting the checkbox size */
      input[type='checkbox'] {
        transform: scale(1.5);
        margin-right: 20px;  /* This gives some spacing between the checkbox and the label */
      }
     
      /* Adjusting the label font size */
      .custom-checkbox-label {
        font-size: 14px; 
        margin-left: 10px;
      }
    ",
                 # Define background color for header bar
                 HTML("/* logo */
                                .skin-blue .main-header .logo {
                                background-color: #03112F;
                                }
                                
                                /* logo when hovered */
                                .skin-blue .main-header .logo:hover {
                                background-color: #03112F;
                                }
                                
                                /* navbar (rest of the header) */
                                .skin-blue .main-header .navbar {
                                background-color: #03112F;
                                }"), 
                 HTML(".main-header .logo {
          background-color: #5B99D6;  /* Blue color from Adrians icon*/
          color: white;               /* Text color */
          text-align: center;         /* Centered text */
          height: 60px;               /* Adjust height as needed */
          line-height: 60px;          /* Center text vertically */
          width: 100%;                /* Full width */
          border-bottom: 1px solid #367fa9;
    }
.modal-content {
         font-size: 16px;
        }
        .modal-header {
        background-color: #f7f7f7;
      }

.main-header .navbar {
          display: none;
          background-color: #5B99D6;
}
        .custom-sidebar {
          position: fixed;
          top: 0;
          left: 0;
          bottom: 0;
          min-width: 250px;
          overflow-y: auto;
          background-color: #f1f1f1;
          z-index: 1000; 
          padding-left: 20px;  
          padding-top: 80px;
        }
         .scrollable-content {
          max-height: 300px;
          overflow-y: auto;
        }
        .flex-container {
          display: flex;
          justify-content: space-between;
          align-items: center; 
          flex-wrap: wrap;
          width: 100%;
        }
        .submit-button {
         margin-top: 10px;
         margin-right 10px;
        }
        
        .footer-section {
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background-color: #03112F; /* or any color you prefer */
            color: white;
            text-align: center;
            padding: 10px 0;
            z-index: 1000;
        }
        .footer-logo {
    height: 30px; 
    margin-right: 15px;
    vertical-align: middle;
}
.footer-content {
    display: inline-block;
    vertical-align: middle;
}
             
             #.skin-blue .main-header .navbar {background-color: transparent;
            # top: 50px !important; }
             
             .main-sidebar {background-color: transparent;
             top: 60px !important;}
             
             .content-wrapper {background-color: transparent;}
             #.main-footer {background-color: transparent;}
             .container-fluid {max-width: 100%;}
             .leaflet {height: calc(100vh - 80px) !important;}
             .leaflet-control {background: none !important;}
             .sidebarCollapsed {width: 45%; background: rgba(255, 255, 255, 0.9); border: 0px;}
             .sidebarCollapsedNotHovered {width: 45%; background: rgba(255, 255, 255, 0.6); border: 0px;}
             .stationSidebar {width: 25%; background: rgba(255, 255, 255, 0.5); border: 0px;}
             .stationSidebarHovered {width: 25%; background: rgba(255, 255, 255, 0.9); border: 0px;}
            .shiny-notification {
        position: fixed !important;
        bottom: 63px !important;
        left: 13px !important;
        width: 15% !important;
            }
      
      "))
    ),
    div(class = "custom-sidebar",
        div(class = "flex-container",
            pickerInput(
              "station",
              "Выберите гидропост:",
              choices = NULL,
              options = list(`actions-box` = TRUE,`live-search` = TRUE),
              multiple = TRUE
            ),
            actionButton("submit", "Выбрать", icon = icon("check-circle"), class = "submit-button")
        ),
        tags$h5("Выбранные гидропосты для прогноза:", style = "font-weight: bold; font-size: 16px;"),
        div(class = "scrollable-content",
            uiOutput("current_selected_stations_ui"),
        ),
        uiOutput("excel_checkbox_ui"),
        #actionButton("show_docs", "Documentation")
        
    ),#div
    
    div(style = "margin-left: 250px;",
        leafletOutput("map", width = "100%", height = "100%"),
        absolutePanel(id = "controls", class = "sidebarCollapsed sidebarCollapsedNotHovered", fixed = TRUE,
        ) #absolutePanel
    ),#div of the table
    
    tags$div(class = "footer-section", 
             #shiny::img(src = "hsol_logo.png", class = "footer-logo"),
             tags$div(class="footer-content",
                      "© 2023 Проект SAPPHIRE Cenetral Asia Разработано hydrosolutions GmbH при поддержке Швейцарского агентства развития и сотрудничества",
                      #tags$div(class="footer-contact", 
                      #         "Вебсайт: ", 
                      #         "www.hydrosolutions.ch | Телефон: +41 43 535 05 80 | Электронна почта: hs@hydrosolutions.ch"
                      #),
             ))
    
  ),#body
  
  
)#Page


# SERVER
server <- function(input, output, session){
  
  # shiny::setTitle("Выбор гидропостов для пентадного прогноза")
  
  
  # Help and instructions
  observeEvent(input$show_docs, {
    showModal(modalDialog(
      title = "Руководство по использованию панели для выбора Гидропостов для пентадного прогнозирования",
      HTML("
    <h3>Добро Пожаловать!</h3>
    <p>Эта панель предназначена для выбора Гидропостов для пятидневного (пентада) прогнозирования.  </p>

    <h4>Обзор панели </h4>
    <ul>
      <li><strong>Карта</strong>: Этот компонент информационной панели находится в центре. Она показывает географическое расположение гидропостов. Вы можете приближать или отдалять карту крутя колесиком мышки. А также увидеть код станции и имя если навести мышку на гидропост на карте.</li>
      <li><strong>Выбор гидропостов</strong>: Расположен в левой части панели. Этот элемент позволяет вам выбрать гидропосты которые вы хотите включить для прогнозирования. Вы можете искать и выбирать из списка доступный гидропостов для пентадного прогноза.</li>
      <li><strong>Выбранные гидропосты</strong>: Под выбором гидропостов расположен список, где отображаются гидропосты уже выбранные для прогнозирования.</li>
      <li><strong>Опция экспорта в Эксель</strong>:  Если вы хотите экспортировать данные выбранных Гидропостов в традиционном формате для выполнения линейной регрессии, вы можете использовать опцию экспорта в Excel.
      Просто отметьте соответствующий пункт, чтобы активировать эту функцию. Если стоит галочка в квадрате ☑, то система прогнозирования создаст файл Эксель с данными прогноза для каждого выбранного Гидропоста при следующем регулярном пятидневном прогнозе.</li>
    </ul>

    <h4 style='color: red;'>Важно: Требуется подтверждение</h4>
    <p style='background-color: yellow;'><strong>Подтверждение</strong>: После совершения выбора вам необходимо <strong>подтвердить изменения</strong> на информационной панели выбора Гидропостов кнопкой 'Подтвердить'.
    Вы получите уведомления, подтверждающие успех ваших действий, в нижнем левом углу окна информационной панели. Если вы НЕ подтвердите изменения в выборе Гидропостов, изменения НЕ будут применены.</p>

    <h3>Пошаговое руководство по использованию панели пентадного прогнозирования Гидропостов</h3>
<p>Следуйте этим шагам, чтобы эффективно использовать панель для выбора Гидропостов и создания пентадного прогноза..</p>


<h4>Step 1: Выберите Гидропосты</h4>
<p>На левой панели вы найдете выпадающий список для выбора Гидропостов (прямоугольное поле).
Здесь вы можете кликнуть и увидеть список Гидропостов. Поищите и выберите нужные вам Гидропосты для создания пятидневных прогнозов или снимите выбор с тех, для которых вам не нужно создавать прогнозы.
Нажмите на Гидропосты, которые вам нужно выбрать. Вы можете выбрать несколько Гидропостов, нажимая на них. Вы можете использовать поиск, чтобы найти нужные Гидропосты, вводя цифровой код или его название.</p>

<h4>Step 2: Осмотрите свой выбор</h4>
<p> Просмотрите выбранные вами Гидропосты. Выбранные Гидропосты будут выделены на карте для удобства.</p>

<h4 style='color: red;'>Step 3: Подтвердите свой выбор </h4>
<p style='background-color: yellow;'><strong>Это критически важный шаг.</strong>
Как только вы удовлетворены своим выбором, нажмите кнопку 'Выбрать', чтобы выбрать эти гидропосты Появится всплывающее окно с запросом на подтверждение. Нажмите 'Подтвердить', чтобы продолжить, или 'Отмена', если что-то не так.</p>

<h4>Step 4: Активируйте или деактивируйте запись в Экселе </h4>
<p>Если вам требуется файл Excel с данными прогноза в традиционном формате, используемом для пятидневного прогнозирования сброса, установите галочку, чтобы активировать эту функцию. Если не нужен, то уберите галочку</p>

<h4>Step 5: Просмотрите ваш выбор снова</h4>
<p>Проверьте выбранные вами Гидропосты и экспорт в Excel ☑. Вы можете закрыть и снова открыть приложение, чтобы убедиться, что изменения применены.</p>

           "),
      size = "l"  # Set the size of the modal. Options: c("s", "m", "l")
    ))
  })
  
  
  
  
  
  
  # Map  
  output$map <- renderLeaflet({
    leaflet(data = stations_df,options = leafletOptions(zoomControl = FALSE )) %>%
      addTiles() %>%
      addPolygons(data = shp_file,
                  color = "#444444",
                  weight = 1,
                  smoothFactor = 0.5,
                  fillOpacity = 0.01,
                  opacity = 1) %>%
      addCircleMarkers(
        lng = ~long, lat = ~lat,
        fillColor = "gray", #~pal(),
        color = "black", # Set outline color to black
        fillOpacity = 0.8,
        weight = 2, # Set outline thickness
        radius = 8,
        label = ~paste(id, name_ru),
        labelOptions = labelOptions(direction = 'auto'))
    
  });
  
  #remember what stations were selected
  
  selected_stations_from_json <- reactive({
    config_outputs_Q <- fromJSON(paste0(config_dir,"/",config_station_selection_file_name))
    config_outputs_Q$stationsID
  })
  
  
  observe({
    # This will run once when the app starts
    updatePickerInput(session, "station", selected = selected_stations_from_json)
  })
  
  #highlighted stations on map when selected on the panel on the left
  selectedStations <- reactive({
    if (is.null(input$station)) {
      return(NULL)
    } else {
      stations_df[stations_df$id %in% gsub("\\s—.*", "", input$station), ]
    }
  })
  observe({
    if (is.null(selectedStations())) {
      # No stations are selected, clear highlighted markers
      leafletProxy("map") %>%
        clearGroup("selectedStations")
    } else {
      # Some stations are selected, update highlighted markers
      leafletProxy("map") %>%
        clearGroup("selectedStations") %>%
        addCircleMarkers(
          data = selectedStations(),
          lng = ~long, lat = ~lat,
          fillColor = "red", # Red color for selected stations
          color = "black",
          fillOpacity = 1,
          weight = 2,
          radius = 11, # Bigger radius for selected stations
          label = ~as.character(id),
          labelOptions = labelOptions(direction = 'auto'),
          group = "selectedStations" # Add to the group of selected stations
        )
    }
  })
  
  
  #existing stations part (load the stations that are selected for forecast from the JSON)
  selected_station_ids <- reactiveVal()
  
  load_config_outputs <- function() {
    config_outputs <- fromJSON(paste0(config_dir,"/",config_station_selection_file_name))
    selected_station_ids(config_outputs$stationsID)
  }
  # Load the JSON file initially
  load_config_outputs()
  # Reload the JSON file when the "confirmation" button on pop up window is clicked
  observeEvent(input$confirm_save, {
    selected_station_ids(gsub("\\s—.*", "", input$station))
  })
  
  observe({
    updatePickerInput(
      session, 
      inputId = "station", 
      selected = selected_station_ids()
    )
  })
  
  # UI to display the currently selected stations
  output$current_selected_stations_ui <- renderUI({
    # Get the IDs of the currently selected stations
    current_selected_ids <- selected_station_ids()
    
    if (is.null(current_selected_ids) || length(current_selected_ids) == 0) {
      return("Нет выбранных гидропостов для прогноза.")
    } else {
      # Find the names corresponding to these IDs
      current_selected_names <- sapply(current_selected_ids, function(x) station_library[["stations_available_for_forecast"]][[x]][["name_ru"]])
      
      # Combine IDs and names
      current_selected_info <- paste(current_selected_ids, current_selected_names, sep = " - ")
      
      # Display the information
      HTML(paste("<p>", current_selected_info, "</p>", collapse = ""))
    }
  })
  
  
  
  # JSON modification (edit the json files when station submit button clicked or Excel checkbox clicked)
  ## reactive values for config
  station_selection_rv <- reactiveVal(fromJSON(paste0(config_dir,"/",config_station_selection_file_name)))
  #station selection
  choices <- setNames(names(station_library$stations_available_for_forecast), 
                      sapply(names(station_library$stations_available_for_forecast), 
                             function(x) paste(x, 
                                               station_library$stations_available_for_forecast[[x]]$name_ru, 
                                               sep = " — ")))
  updatePickerInput(session, "station", choices = choices)
  
  observeEvent(input$submit, {
    showModal(modalDialog(
      title = "Подтверждение",
      "Уверены ли вы в изменениях?",
      footer = tagList(
        actionButton("confirm_save", "Подтвердить"),
        modalButton("Отменить")
      )
    ))
  })
  
  observeEvent(input$confirm_save, {
    station_data <- station_selection_rv()
    station_data$stationsID <- gsub("\\s—.*", "", input$station)
    station_selection_rv(station_data)
    
    jsonlite::write_json(station_selection_rv(), paste0(config_dir,"/",config_station_selection_file_name), pretty = TRUE)
    
    showNotification("Гидропост(ы) выбраны успешно!", type = "message")
    removeModal()
  })
  
  
  # Excel checkbox
  
  
  updateCheckboxInput(session, "write_excel", value = excel_config$write_excel)
  
  excel_checkbox_rv <- reactiveVal(excel_config$write_excel)
  
  observeEvent(input$dynamic_write_excel, {
    excel_config$write_excel <- input$dynamic_write_excel
    jsonlite::write_json(
      excel_config, 
      paste0(config_dir,"/",config_output_file_name), 
      pretty = TRUE, 
      flatten = TRUE, 
      auto_unbox = TRUE)
    updated_excel_config <- fromJSON(paste0(config_dir,"/",config_output_file_name))
    excel_checkbox_rv(updated_excel_config$write_excel)
  })
  
  
  # UI rendering for Excel checkbox
  
  output$excel_checkbox_ui <- renderUI({ 
    excel_config <- fromJSON(paste0(config_dir,"/",config_output_file_name))
    div(class = "custom-checkbox-label",
        checkboxInput("dynamic_write_excel", label = "Записать Эксель файл", 
                      value = excel_config$write_excel)
    )
  })
  
  
  
  
}



shinyApp(ui = ui, server = server, options = list(launch.browser = FALSE))