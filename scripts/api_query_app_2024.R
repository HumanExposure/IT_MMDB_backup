## This is a Shiny web application. You can run the application by clicking the 'Run App' button above.
## Authors: Rachel Hencher
## Created: 2024-10-29
## Last updated: 2024-10-29

######################################################################
# Set Up
######################################################################

# Install required packages
required.packages = c('shiny', 'shinydashboard', 'shinythemes', 'DT', 'DBI', 'RPostgreSQL', 'dplyr', 'dbplyr', 'reshape', 'data.table', 'ctxR')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

######################################################################
# UI
######################################################################

# Define UI for application
ui <- navbarPage("MMDB Querying Tool", tags$style(HTML(".navbar-brand { font-size: 32px; }")),
                 
                 # Set theme
                 theme = shinytheme("cerulean"),
                 
                 # Set up structure of "CONNECT" tab
                 tabPanel("CONNECT",
                          
                          # Connection to CCTE APIs database using registered API key
                          HTML("<h3>Store an API key for the current session by entering your unique key below</h3>"),
                          HTML("<h5><i>An API key can be obtained at no cost by emailing ccte_api@epa.gov</i></h5>"),
                          br(),
                          textInput("my_key", "API key", value = "your_api_key"),
                          actionButton("store", "Store API key for this session"),
                          br(),
                          br(),
                          
                          textOutput("status_text"), tags$style("#status_text { font-size: 16px; color: red; }")
                 ),
                 
                 # First of two categories for querying
                 tabPanel("QUERY BY DTXSID",
                          HTML("<h3><b>The following will return selected information for the list of chemicals uploaded</b></h3>"),
                          br(),
                          
                          # Upload and preview a chemical list to subset by
                          HTML("<h3>To query by chemical list (as DTXSIDs), first upload the chemical list below</h3>"),
                          fileInput("file", "Chemical list", buttonLabel = "Upload..."),
                          textInput("delim", "Delimiter (leave blank to guess)", ""),
                          numericInput("skip", "Rows to skip", 0, min = 0),
                          br(),
                          HTML("<h3>After uploading a chemical list, preview before proceeding to ensure correct formatting</h3>
                                <h4>The preview should display a single column of DTXSIDs with 'DTXSID' as the variable name</h4>"),
                          actionButton("preview_button", "Preview upload"),
                          br(),
                          br(),
                          DTOutput("preview"),
                          br(),
                          
                          # Download button to be used after all selections are made
                          downloadButton("download", "Download results")),
                 
                 
                 # Second of two categories for querying
                 tabPanel("QUERY BY MEDIA",
                          HTML("<h3><b>The following will return selected information for the selected media</b></h3>"),
                          br())
)

######################################################################
# Server
######################################################################

# Define server logic
server <- function(input, output, session) {
  
  ## Connect Tab           
  
  # After selecting 'store', a message is displayed detailing whether the connection was successful
  observeEvent(input$store, {
    # This function stores the key in the current session
    register_ctx_api_key(key = 'input$my_key')
    
    if (grepl('[\"\'"]', input$my_key)) {
      output$status_text <- renderText("Remove any quotations and submit again.") 
      } else { 
        output$status_text <- renderText("Thank you for your submission!")
    }
  })
  
  ## Query by DTXSID Tab 
  
  # When preview button is selected, the chemical list csv file is read in according to the specified delimiter 
  # The indicated number of rows will be skipped such that the first chemical appears in row 1
  # The variable in column A will be called DTXSID
  # A datatable will then be generated to allow the user to preview the upload
  upload_data <- eventReactive(input$preview_button, {
    req(input$file)
    inFile <- input$file
    delimiter <- ifelse(input$delim == "", ",", input$delim)
    data <- read.csv(inFile$datapath, sep = delimiter, header = FALSE, skip = input$skip)
    names(data)[1] <- "DTXSID"
    return(data)
  })
  
  output$preview <- renderDT({
    if(input$preview_button){
      upload_data()
    }
  })
  
  # ???
  output$download <- downloadHandler({
    DTXSIDs <- upload_data()
    DTXSIDs <- DTXSIDs %>% 
      pull(DTXSID) %>% 
      sprintf('"%s"', .) %>% 
      paste(collapse = ", ") %>% 
      paste("c(", ., ")", sep = "")
    data <- get_exposure_functional_use_batch(DTXSIDs)
    write.csv(x = data, file = "MMDB_download_by_DTXSID.csv", row.names = FALSE)
    })
  

}

# Run the application 
shinyApp(ui = ui, server = server)