## This is a Shiny web application. You can run the application by clicking the 'Run App' button above.
## Authors: Rachel Hencher
## Created: 2024-??-??
## Last updated: 2024-08-08

######################################################################
# Set Up
######################################################################

# Install required packages
required.packages = c('shiny', 'shinydashboard', 'shinythemes', 'DT', 'DBI', 'RPostgreSQL', 'dplyr', 'dbplyr', 'reshape', 'data.table', 'openxlsx', 'readxl')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Link to 'Supplementary information' for metadata
var_map_url <- "https://static-content.springer.com/esm/art%3A10.1038%2Fs41597-022-01365-8/MediaObjects/41597_2022_1365_MOESM3_ESM.xlsx"
vars_url <- "https://static-content.springer.com/esm/art%3A10.1038%2Fs41597-022-01365-8/MediaObjects/41597_2022_1365_MOESM2_ESM.xlsx"
source_url <- "https://static-content.springer.com/esm/art%3A10.1038%2Fs41597-022-01365-8/MediaObjects/41597_2022_1365_MOESM1_ESM.xlsx"
media_map_url <- "https://static-content.springer.com/esm/art%3A10.1038%2Fs41597-022-01365-8/MediaObjects/41597_2022_1365_MOESM5_ESM.xlsx"
chem_map_url <- "https://static-content.springer.com/esm/art%3A10.1038%2Fs41597-022-01365-8/MediaObjects/41597_2022_1365_MOESM4_ESM.xlsx"

# The following information will need updated as new media types, new sources, new variables, etc. are added to the database
# List of media pt 1 (to display in two columns)
media1_list <- c("Ambient air" = "ambient air",
                 "Indoor air" = "indoor air", 
                 "Personal air" = "personal air", 
                 "Breast milk" = "breast milk",
                 "Human blood (whole/serum/plasma)" = "human blood (whole/serum/plasma)",
                 "Human (other tissues/fluids)" = "human (other tissues or fluids)", 
                 "Food product" = "food product", 
                 "Livestock/meat" = "livestock/meat", 
                 "Indoor dust" = "indoor dust", 
                 "Product" = "product", 
                 "Raw agricultural commodity" = "raw agricultural commodity", 
                 "Sediment" = "sediment", 
                 "Soil" = "soil", 
                 "Skin wipes" = "skin wipes", 
                 "Urine" = "urine", 
                 "Vegetation" = "vegetation")

# List of media pt 2 (to display in two columns)
media2_list <- c("Drinking water" = "drinking water",
                 "Groundwater" = "groundwater", 
                 "Surface water" = "surface water", 
                 "Wastewater (influent/effluent)" = "wastewater (influent, effluent)",
                 "Precipitation" = "precipitation", 
                 "Sludge" = "sludge", 
                 "Wildlife (aquatic invertebrate)" = "wildlife (aquatic invertebrate)",
                 "Wildlife (fish)" = "wildlife (fish)", 
                 "Wildlife (aquatic vertebrates/mammals)" = "wildlife (aquatic vertebrates/mammals)", 
                 "Wildlife (birds)" = "wildlife (birds)", 
                 "Wildlife (terrestrial invertebrates/worms)" = "wildlife (terrestrial invertebrates/worms)",
                 "Wildlife (terrestrial vertebrates)" = "wildlife (terrestrial vertebrates)", 
                 "Landfill leachate" = "landfill leachate", 
                 "Other ecological" = "other-ecological", 
                 "Other environmental" = "other-environmental",
                 "Other personal" = "other-personal",
                 "Unknown" = "unknown")

# List of variables (common to both aggregate & single-sample) pt 1 (to display in three columns)
vars1_list <- c("Preferred chemical name" = "preferred_name",
                "Reported chemical name" = "reported_chemical_name",
                "DTXSID" = "dtxsid",
                "Harmonized media" = "harmonized_medium",
                "Reported media" = "reported_media", 
                "Reported species" = "reported_species",
                "Full source name" = "full_source_name",
                "Source name abbreviation" = "source_name_abbr",
                "Source description" = "description",
                "CASRN" = "casrn",
                "How curated" = "howcurated",
                "DTXRID" = "dtxrid",
                "Substance type" = "substance_type",
                "InChIKey" = "structure_inchikey",
                "Structure formula" = "structure_formula")

# List of aggregate variables pt 2 (to display in three columns)
agg_vars2_list <- c("Detect status" = "detected",
                    "Detect status notes" = "detect_note", 
                    "Detect conflict flag" = "detect_conflict",
                    "QC flag(s)" = "qc_flag",
                    "Reported statistic" = "reported_statistic",
                    "Reported result" = "reported_result",
                    "Reported units" = "reported_units",
                    "LoQ" = "loq", 
                    "LoD" = "lod",
                    "Reported n" = "reported_n",
                    "Reported # detects" = "reported_num_detects", 
                    "Reported # non-detects" = "reported_num_nds", 
                    "Reported detect rate" = "reported_detect_rate", 
                    "Reported detect rate description" = "reported_detect_rate_description",
                    "Reported non-detect rate" = "reported_nondetect_rate", 
                    "Reported non-detect rate description" = "reported_nondetect_rate_description")

# List of aggregate variables pt 3 (to display in three columns)  
agg_vars3_list <- c("Other limit" = "other_limit",
                    "Other limit description" = "other_limit_description",
                    "Reported data ID" = "reported_data_id",
                    "Reported collection activity ID" = "reported_collection_activity_id",
                    "Reported population" = "reported_population",
                    "Reported subpopulation" = "reported_subpopulation",
                    "Reported dates" = "reported_dates",
                    "Reported years" = "years",
                    "Reported gender" = "reported_gender",
                    "Reported location" = "reported_location",
                    "Country" = "country",
                    "State or province" = "state_or_province",
                    "US county" = "us_county")

# List of raw variables pt 2 (to display in three columns)
raw_vars2_list <- c("Detect status" = "detected",
                    "Detect status notes" = "detect_note", 
                    "Detect conflict flag" = "detect_conflict",
                    "QC flag(s)" = "qc_flag",
                    "Reported result" = "reported_result",
                    "Reported units" = "reported_units",
                    "LoQ" = "loq", 
                    "LoD" = "lod",
                    "Result flag" = "result_flag",
                    "Non-detect flag" = "nd_flag")

# List of raw variables pt 3 (to display in three columns)
raw_vars3_list <- c("Other limit" = "other_limit",
                    "Other limit description" = "other_limit_description",
                    "Reported sample ID" = "reported_sample_id",
                    "Reported collection activity ID" = "reported_collection_activity_id",
                    "Sample month" = "sample_month",
                    "Sample year" = "sample_year",
                    "Reported date" = "reported_date",
                    "Reported location" = "reported_location",
                    "Country" = "country",
                    "State or province" = "state_or_province",
                    "US county" = "us_county",
                    "Reported reference" = "reported_reference")

# List of aggregate data sources
agg_source_list <- c("Comparative Toxicogenomics Database"= "ctd",
                     "American Healthy Homes Survey" = "ahhs",
                     "Biomonitoring California" = "biomon_ca",
                     "California Air Monitoring Network" = "ca_airmon",
                     "California Air Resources Board (CARB)" = "carb",
                     "EPA Office of Water, National study of Chemical Residues in Lake Fish Tissue" = "epa_nscrlft",
                     "National Health and Nutrition Examination Survey" = "nhanes",
                     "EPA Discharge Monitoring Report Data" = "epa_dmr",
                     "Information Platform for Chemical Monitoring Data - Lakes and Rivers" = "ip_chem_lakes",
                     "Information Platform for Chemical Monitoring Data - Biomonitoring" = "ip_chem_biomonitoring")

# List of raw data sources
raw_source_list <- c("Chem Theatre" = "chem_theatre",
                     "EPA Nine POTW Study" = "epa_9potw",
                     "USGS Monitoring Data National Water Quality Monitoring Council - Air, Soil, Biological (Tissue), Sediment, Water" = "usgs",
                     "California Surface Water Database - Sediment" = "ca_surf_sediment",
                     "California Surface Water Database - Surface Water" = "ca_surf_water",
                     "EPA Ambient Monitoring Technology Information Center - Air Toxics Data" = "epa_amtic",
                     "EPA Targeted National Sewage Sludge Survey" = "epa_tnsss",
                     "EPA Unregulated Contaminant Monitoring Rule" = "epa_ucmr",
                     "FDA Total Diet Study -  elements" = "fda_tds_elem",
                     "FDA Total Diet Study -  pesticides" = "fda_tds_pest",
                     "ICES-Dome Biota" = "ices_biota",
                     "National Atmospheric Deposition Program (Atmospheric Integrated Research Monitoring Network)" = "airmon",
                     "ICES-Dome Sediment" = "ices_sediment",
                     "Information Platform for Chemical Monitoring Data - Biota" = "ip_chem_biota",
                     "Information Platform for Chemical Monitoring Data - Seawater" = "ip_chem_seawater",
                     "Information Platform for Chemical Monitoring Data - Sediment" = "ip_chem_sediment",
                     "USDA National Residue Program" = "usda_nrp",
                     "Information Platform for Chemical Monitoring Data - IBS" = "ip_chem_ibs")

######################################################################
# UI
######################################################################

# Define UI for application
ui <- navbarPage("MMDB Querying Tool", tags$style(HTML(".navbar-brand { font-size: 32px; }")),
                 
                 # Set theme
                 theme = shinytheme("cerulean"),
                 
                 # Set up structure of "CONNECT" tab
                 tabPanel("CONNECT",
                          
                          # Connection to res_mmdb database using username and password
                          HTML("<h3>Enter your credentials to connect to the database:</h3>"),
                          textInput("user", "User", value = "your_username"),
                          textInput("password", "Password", value = "your_password"),
                          textInput("port", "Port", value = "5432"),
                          textInput("host", "Host", value = "ccte-postgres-res.dmap-prod.aws.epa.gov"),
                          textInput("dbname", "Database", value = "res_mmdb"),
                          actionButton("connect", "Connect"),
                          br(),
                          br(),
                          
                          # Test to report whether connection was successful
                          textOutput("connection_status"), tags$style("#connection_status { font-size: 20px; color: red; }")
                 ),
                 
                 # Set up structure of "QUERY" tab
                 navbarMenu("QUERY",
                            
                            # First of three sub-categories for querying            
                            tabPanel("CUSTOM QUERY",
                                     
                                     # Option to upload and preview a chemical list to subset by
                                     HTML("<h3>To query by chemical list (as DTXSIDs), first upload the chemical list below</h3>
                                <h4><i>*Optional*</i></h4>"),
                                     fileInput("file", "Chemical list", buttonLabel = "Upload..."),
                                     textInput("delim", "Delimiter (leave blank to guess)", ""),
                                     numericInput("skip", "Rows to skip", 0, min = 0),
                                     br(),
                                     HTML("<h3>If uploading a chemical list, preview before proceeding to ensure correct formatting</h3>
                                <h4>The preview should display a single column of DTXSIDs with 'DTXSID' as the variable name</h4>"),
                                     actionButton("preview_button", "Preview upload"),
                                     br(),
                                     DTOutput("preview"),
                                     br(),
                                     
                                     # Option to subset by specified media types
                                     HTML("<h3>Select all media of interest</h3>"),
                                     actionButton("select_all", "Select all", style = 'padding:4px; font-size:85%'),
                                     br(),
                                     br(),
                                     fluidRow(column(width = 4, checkboxGroupInput("media1", "Media types:", 
                                                                                   choices = media1_list, 
                                                                                   inline = FALSE)),
                                              column(width = 5, checkboxGroupInput("media2", "", 
                                                                                   choices = media2_list, 
                                                                                   inline = FALSE))),
                                     
                                     # Option to select whether data is of type aggregate or single-sample
                                     HTML("<h3>Select which type of data to generate</h3>"),
                                     selectInput("type", "Data type:", choices = c("Aggregate", "Single-sample")),
                                     br(),
                                     
                                     # If 'aggregate' selected above, then aggregate variables and aggregate sources appear to subset by
                                     # Action button for option to select all variables
                                     # All sources pre-selected with the option to deselect 
                                     conditionalPanel(condition = "input.type == 'Aggregate'",
                                                      HTML("<h3>Select all variables of interest</h3>"),
                                                      actionButton("select_all_agg", "Select all", style = 'padding:4px; font-size:85%'),
                                                      br(),
                                                      br(),
                                                      fluidRow(column(width = 4, checkboxGroupInput("agg_vars1", "Variables:", 
                                                                                                    choices = vars1_list,
                                                                                                    selected = c("preferred_name",
                                                                                                                 "reported_chemical_name",
                                                                                                                 "dtxsid", 
                                                                                                                 "harmonized_medium", 
                                                                                                                 "reported_media",
                                                                                                                 "source_name_abbr"),
                                                                                                    inline = FALSE)),
                                                               column(width = 4, checkboxGroupInput("agg_vars2", "", 
                                                                                                    choices = agg_vars2_list,
                                                                                                    selected = c("detected",
                                                                                                                 "detect_note",
                                                                                                                 "reported_statistic",
                                                                                                                 "reported_result",
                                                                                                                 "reported_units"),
                                                                                                    inline = FALSE)),
                                                               column(width = 4, checkboxGroupInput("agg_vars3", "", 
                                                                                                    choices = agg_vars3_list, 
                                                                                                    inline = FALSE))),
                                                      HTML("<h3>Select all sources of interest</h3>"),
                                                      fluidRow(column(width = 12, checkboxGroupInput("agg_sources", "Sources:", 
                                                                                                     choices = agg_source_list,
                                                                                                     selected = agg_source_list,
                                                                                                     inline = FALSE)))),
                                     
                                     # If 'single-sample' selected above, then raw variables and raw sources appear to subset by
                                     # Action button for option to select all variables
                                     # All sources pre-selected with the option to deselect
                                     conditionalPanel(condition = "input.type == 'Single-sample'",
                                                      HTML("<h3>Select all variables of interest</h3>"),
                                                      actionButton("select_all_raw", "Select all", style = 'padding:4px; font-size:85%'),
                                                      br(),
                                                      br(),
                                                      fluidRow(column(width = 4, checkboxGroupInput("raw_vars1", "Variables:", 
                                                                                                    choices = vars1_list,
                                                                                                    selected = c("preferred_name",
                                                                                                                 "reported_chemical_name",
                                                                                                                 "dtxsid", 
                                                                                                                 "harmonized_medium", 
                                                                                                                 "reported_media",
                                                                                                                 "source_name_abbr"),
                                                                                                    inline = FALSE)),
                                                               column(width = 4, checkboxGroupInput("raw_vars2", "", 
                                                                                                    choices = raw_vars2_list,
                                                                                                    selected = c("detected",
                                                                                                                 "detect_note",
                                                                                                                 "reported_result",
                                                                                                                 "concentration",
                                                                                                                 "reported_units"),
                                                                                                    inline = FALSE)),
                                                               column(width = 4, checkboxGroupInput("raw_vars3", "", 
                                                                                                    choices = raw_vars3_list, 
                                                                                                    inline = FALSE))),
                                                      HTML("<h3>Select all sources of interest</h3>"),
                                                      fluidRow(column(width = 12, checkboxGroupInput("raw_sources", "Sources:",
                                                                                                     choices = raw_source_list,
                                                                                                     selected = raw_source_list,
                                                                                                     inline = FALSE)))),
                                     br(),
                                     
                                     # Option to subset by detected status types
                                     HTML("<h3>Indicate whether to subset by detected status</h3>
                                <h4><i>Detect status</i> must be selected above</h4>"),
                                     checkboxInput("detect", "Show only unambiguous detects"),
                                     br(),
                                     
                                     # Download button to be used after all selections are made
                                     downloadButton("download", "Download results"),
                                     br(),
                                     br()),
                            
                            # Second of three sub-categories for querying
                            tabPanel("QUERY BY DTXSID",
                                     HTML("<h3><b>The following will return all information (or a summary if selected) for the list of chemicals uploaded</b></h3>"),
                                     br(),
                                     
                                     # Upload and preview a chemical list to subset by
                                     HTML("<h3>To query by chemical list (as DTXSIDs), first upload the chemical list below</h3>"),
                                     fileInput("file2", "Chemical list", buttonLabel = "Upload..."),
                                     textInput("delim2", "Delimiter (leave blank to guess)", ""),
                                     numericInput("skip2", "Rows to skip", 0, min = 0),
                                     br(),
                                     HTML("<h3>After uploading a chemical list, preview before proceeding to ensure correct formatting</h3>
                                <h4>The preview should display a single column of DTXSIDs with 'DTXSID' as the variable name</h4>"),
                                     actionButton("preview_button2", "Preview upload"),
                                     br(),
                                     br(),
                                     DTOutput("preview2"),
                                     
                                     # Option to select whether data is of type aggregate or single-sample
                                     HTML("<h3>Select which type of data to generate</h3>"),
                                     selectInput("type2", "Data type:", choices = c("Aggregate", "Single-sample")),
                                     br(),
                                     
                                     # Option to return a summary of the data instead of full detailed results
                                     HTML("<h3>Indicate whether to return only a summary of the results</h3>
                                <h4>Leave blank to return full detailed information</h4>"),
                                     checkboxInput("summary", "Return summary information"),
                                     br(),
                                     
                                      # Download button to be used after all selections are made
                                     downloadButton("download2", "Download results"),
                                     br(),
                                     br()),
                            
                            # Third of three sub-categories for querying
                            tabPanel("QUERY BY MEDIA",
                                     HTML("<h3><b>The following will return a list of all chemicals (preferred name and DTXSID) that were detected within the selected media</b></h3>"),
                                     br(),
                                     
                                     # Select media types to subset by
                                     HTML("<h3>Select all mediums of interest</h3>"),
                                     fluidRow(column(width = 4, checkboxGroupInput("media3", "Media types:", 
                                                                                   choices = media1_list, 
                                                                                   inline = FALSE)),
                                              column(width = 5, checkboxGroupInput("media4", "", 
                                                                                   choices = media2_list, 
                                                                                   inline = FALSE))),
                                     
                                     # Option to select whether data is of type aggregate or single-sample
                                     HTML("<h3>Select which type of data to generate</h3>"),
                                     selectInput("type3", "Data type:", choices = c("Aggregate", "Single-sample")),
                                     br(),
                                     
                                     # Download button to be used after all selections are made
                                     downloadButton("download3", "Download results"),
                                     br(),
                                     br())),
                 
                 # Set up structure of "METADATA" tab
                 tabPanel("METADATA",
                          
                          # Description and details for mapping of harmonized variables
                          # Action button to display the datatable
                          HTML("<h3>Harmonized variable mappings</h3>
                                <p><i>Mapping of raw data variables to harmonized variables.</i></p>
                                <h5>Variable details:</h5>
                                <ul>
                                <li><strong>Source:</strong> Data source</li>
                                <li><strong>To Harmonized Variable:</strong> Harmonized MMDB variable</li>
                                <li><strong>From Raw Source Variable:</strong> Name of variable in original data source</li>
                                <li><strong>Notes:</strong> Notes on mapping</li>
                                </ul>"),
                          actionButton("view_var_map", "View"),
                          br(),
                          br(),
                          DTOutput("preview_var_map"),
                          br(),
                          
                          # Description and details for variable definitions
                          # Action button to display the datatable
                          HTML("<h3>Variable definitions</h3>
                                <p><i>Descriptions of all database tables and variables.</i></p>
                                <h5>Variable details:</h5>
                                <ul> 
                                <li><strong>Table Name:</strong> Name of database table</li>
                                <li><strong>Variable Name:</strong> Name of table variable</li>
                                <li><strong>Variable Description:</strong> Brief description of variable contents</li>
                                <li><strong>Notes:</strong> Notes including how variable was generated</li>
                                </ul>"),
                          actionButton("view_vars", "View"),
                          br(),
                          br(),
                          DTOutput("preview_vars"),
                          br(),
                          
                          # Description and details for source data
                          # Action button to display the datatable
                          HTML("<h3>Source data</h3>
                                <p><i>Details of the database sources.</i></p>
                                <h5>Variable details:</h5>
                                <ul>
                                <li><strong>Source:</strong> Source description</li>
                                <li><strong>Source (or Source Subset) Abbreviation:</strong> Source abbreviation used in database</li>
                                <li><strong>Citation:</strong> Full citation for source or source subset (in the case of multiple documents or files for source)</li>
                                <li><strong>Phase Collected:</strong> Collection phase of the original data (1: 2016; 2:2017)</li>
                                <li><strong>Extraction Method:</strong> Method of data extraction</li>
                                <li><strong>Single Sample or Aggregated Summary:</strong> Type of data source (single sample or summary)</li>
                                <li><strong>Type of Raw Data:</strong> Type of raw data (e.g., reports/online database)</li>
                                <li><strong>Metadata:</strong> Raw source metadata availability</li>
                                <li><strong>Site of Manual Extraction:</strong> Details of location of manually extracted data (if available)</li>
                                <li><strong>Data and Curation Details:</strong> Notes on curation</li>
                                </ul>"),
                          actionButton("view_source", "View"),
                          br(),
                          br(),
                          DTOutput("preview_source"),
                          br(),
                          
                          # Description and details for mapping of harmonized media
                          # Action button to display the datatable
                          HTML("<h3>Harmonized media mappings</h3>
                                <p><i>Mapping of raw media identifiers to harmonized media.</i></p>
                                <h5>Variable details:</h5>
                                <ul>
                                <li><strong>Source:</strong> Data source</li>
                                <li><strong>Reported_media:</strong> Reported media identifier</li>
                                <li><strong>Reported_species:</strong> Reported species identifier (if applicable)</li>
                                <li><strong>Harmonized_media:</strong> Harmonized medium</li>
                                </ul>"),
                          actionButton("view_media_map", "View"),
                          br(),
                          br(),
                          DTOutput("preview_media_map"),
                          br(),
                          
                          # Description and details for mapping of chemicals
                          # Action button to display the datatable
                          HTML("<h3>Chemical mappings</h3>
                                <p><i>Mapping of raw chemical identifiers to harmonized DTXSID.</i></p>
                                <h5>Variable details:</h5>
                                <ul> 
                                <li><strong>DTXRID:</strong> DSSTox record ID (used by internal EPA data curation system)</li>
                                <li><strong>Source:</strong> Data source</li>
                                <li><strong>Reported_casrn:</strong> Reported CASRN</li>
                                <li><strong>Reported_chemical_name:</strong> Reported chemical name</li>
                                <li><strong>DTXSID:</strong> DSSTOX chemical substance ID</li>
                                <li><strong>PREFERRED_NAME:</strong> Preferred chemical name for this DTXSID</li>
                                <li><strong>CASRN:</strong> Active CASRN associated with this DTXSID</li>
                                <li><strong>Substance_Type:</strong> Type of substance (from EPA curation system)</li>
                                <li><strong>Structure_InChIKey:</strong> InChiKey (from EPA curation system)</li>
                                <li><strong>Structure_Formula:</strong> Structure (from EPA curation system)</li>
                                <li><strong>Howcurated:</strong> How the chemical was curated (Typically EPA/ORD's standard chemical curation workflow)</li>
                                </ul>"),
                          actionButton("view_chem_map", "View"),
                          br(),
                          br(),
                          DTOutput("preview_chem_map")
                 )
)

######################################################################
# Server
######################################################################

# Define server logic
server <- function(input, output, session) {

## Connect Tab           
  
  # A function to connect to the database
  connect_to_mmdb <- function() {
    drv <- dbDriver("PostgreSQL")
    con <- dbConnect(drv,
                     host = input$host,
                     port = as.integer(input$port),
                     dbname = input$dbname,
                     user = input$user,
                     password = input$password)
    return(con)
  }
  
  # After selecting 'connect', a message is displayed detailing whether the connection was successful
  observeEvent(input$connect, {
    con <- connect_to_mmdb()
    dbGetQuery(con, "set search_path to mmdb")
    
    if(inherits(con, "PostgreSQLConnection")) {
      output$connection_status <- renderText({
        "Connection successful!"
      })
    } else {
      output$connection_status <- renderText({
        "Connection failed. Please check your details and try again."
      })
    }
  })
  
## Custom Query Sub-Tab
  
  # When preview button is selected, the chemical list csv file is read in according to the specified delimiter 
  # The indicated number of rows will be skipped such that the first chemical appears in row 1
  # The variable in column A will be called DTXSID
  # A datatable will then be generated to allow the user to preview the upload
  uploaded_data <- eventReactive(input$preview_button, {
    req(input$file)
    inFile <- input$file
    delimiter <- ifelse(input$delim == "", ",", input$delim)
    data <- read.csv(inFile$datapath, sep = delimiter, header = FALSE, skip = input$skip)
    names(data)[1] <- "DTXSID"
    return(data)
  })
  
  output$preview <- renderDT({
    if(input$preview_button){
      uploaded_data()
    }
  })
  
  # If 'select all' is selected, all options for media/variables will be checked for the user; nothing will occur otherwise
  observe({
    if(input$select_all == 0) return(NULL)
    else if(input$select_all > 0) {
      updateCheckboxGroupInput(session, "media1", "Media types:", choices = media1_list, selected = media1_list)
      updateCheckboxGroupInput(session, "media2", "", choices = media2_list, selected = media2_list) 
    }
  })
  
  observe({
    if(input$select_all_agg == 0) return(NULL)
    else if(input$select_all_agg > 0) {
      updateCheckboxGroupInput(session, "agg_vars1", "Variables:", choices = vars1_list, selected = vars1_list)
      updateCheckboxGroupInput(session, "agg_vars2", "", choices = agg_vars2_list, selected = agg_vars2_list) 
      updateCheckboxGroupInput(session, "agg_vars3", "", choices = agg_vars3_list, selected = agg_vars3_list)
    }
  })
  
  observe({
    if(input$select_all_raw == 0) return(NULL)
    else if(input$select_all_raw > 0) {
      updateCheckboxGroupInput(session, "raw_vars1", "Variables:", choices = vars1_list, selected = vars1_list)
      updateCheckboxGroupInput(session, "raw_vars2", "", choices = raw_vars2_list, selected = raw_vars2_list) 
      updateCheckboxGroupInput(session, "raw_vars3", "", choices = raw_vars3_list, selected = raw_vars3_list)
    }
  })
  
  # A function to connect to res_mmdb and subset the data according to the selections by the user
  # A datatable will be returned
  generate_data <- function(data_type) {
    
    # Connect to the database
    con <- connect_to_mmdb()
    dbGetQuery(con, "set search_path to mmdb")
    
    # Collect relevant information from substances, files, source, and media tables
    chems <- tbl(con, "substances") %>% 
      select(-c(external_id, created_dt:record_exp_dt)) %>% 
      collect()
    
    files <- tbl(con, "files") %>% 
      select(-c(location, filename, mapped:record_exp_dt)) %>% 
      collect()
    
    source <- tbl(con, "source") %>% 
      select(-c(data_collection_type, oppt_phase:loaded, created_dt:record_exp_dt)) %>% 
      collect()
    
    media <- tbl(con, "media") %>% 
      select(-c(created_dt:record_exp_dt)) %>% 
      collect()
    
    # Create new variables to reflect the media/sources the user selects, which will be used to subset the data
    selected_media <- media[which(media$harmonized_medium %in% c(input$media1, input$media2)),]
    
    selected_sources <- source[which(source$source_name_abbr %in% c(input$agg_sources, input$raw_sources)),]
    
    # Create a new variable to reflect the chemicals the user uploads, which will be used to subset the data
    # If no file is uploaded, all chemicals from the chems table will be used
    selected_chems <-
      if(is.null(input$file)){
        chems
      } else {
        chems[which(chems$dtxsid %in% uploaded_data()$DTXSID),]
      }
    
    # Join the files and source tables by source_id - to be used later
    join <- left_join(files, source, by = "source_id")
    
    # Conditional statement to reflect whether to search 'aggregate' or 'single-sample' (also known as raw) data
    if(data_type == "Aggregate") {
      
      # Gather data from harmonized_aggregate table
      # Exclude variables created_dt through record_exp_dt because not useful information
      # Filter data based on user inputs for media type, source, and chemical list (if applicable)
      aggregate_data <- tbl(con, "harmonized_aggregate") %>% 
        select(-c(created_dt:record_exp_dt)) %>% 
        filter(media_id %in% !!selected_media$media_id) %>%
        filter(source_id %in% !!selected_sources$source_id) %>%
        filter(substance_id %in% !!selected_chems$substance_id) %>%
        collect()
      
      # Add relevant variables from files, source, media, and chems tables using join statements
      # Subset the data using only the variables selected by the user
      # Filter out designated list of chemicals
      # Return only unique records
      aggregate_data <- left_join(aggregate_data, join) %>%
        left_join(., media[,c("media_id", "harmonized_medium")], by = "media_id") %>%
        left_join(., chems[,c("substance_id", "dtxsid", "preferred_name", "casrn", "howcurated", "dtxrid", "substance_type", "structure_inchikey", "structure_formula")], by = "substance_id") %>%
        select(c(input$agg_vars1, input$agg_vars2, input$agg_vars3)) %>%
        distinct() %>%
        data.table
      
      # If 'show only unambiguous detects' is selected, the data will also be filtered to only show detects
      if(input$detect) {
        aggregate_data <- aggregate_data %>%
          filter(detected > 0)
      }
      
      dbDisconnect(con)
      
      return(aggregate_data)
      
    } else {
      
      # Gather data from harmonized_raw table
      # Exclude variables created_dt through record_exp_dt because not useful information
      # Filter data based on user inputs for media type, source, and chemical list (if applicable)
      raw_data <- tbl(con, "harmonized_raw") %>% 
        select(-c(created_dt:record_exp_dt)) %>% 
        filter(media_id %in% !!selected_media$media_id) %>%
        filter(source_id %in% !!selected_sources$source_id) %>%
        filter(substance_id %in% !!selected_chems$substance_id) %>%
        collect()
      
      # Add relevant variables from files, source, media, and chems tables using join statements
      # Subset the data using only the variables selected by the user
      # Filter out designated list of chemicals
      # Return only unique records
      raw_data <- left_join(raw_data, join) %>%
        left_join(., media[,c("media_id", "harmonized_medium")], by = "media_id") %>%
        left_join(., chems[,c("substance_id", "dtxsid", "preferred_name", "casrn", "howcurated", "dtxrid", "substance_type", "structure_inchikey", "structure_formula")], by = "substance_id") %>%
        select(c(input$raw_vars1, input$raw_vars2, input$raw_vars3)) %>%
        distinct() %>%
        data.table
      
      # If 'show only unambiguous detects' is selected, the data will also be filtered to only show results with detects
      if(input$detect) {
        raw_data <- raw_data %>%
          filter(detected > 0)
      }
      
      dbDisconnect(con)
      
      return(raw_data)
      
    }
  }
  
  # When 'download results' is selected, a csv file will be downloaded locally with a title of "MMDB_download_type_Aggregate" or "MMDB_download_type_Single-sample"
  # While the file is downloading, a message will appear; once the download is complete, the message will disappear
  output$download <- downloadHandler(
    filename = function() {
      paste("MMDB_download_type_", gsub(" ", "_", input$type), ".csv", sep = "")
    },
    
    content = function(filename) {
      showModal(modalDialog("Downloading... This may take several minutes", footer = NULL))
      on.exit(removeModal())
      
      data <- generate_data(input$type)
      write.csv(x = data, file = filename, row.names = FALSE)
    })
  
## Query by DTXSID Sub-Tab  
  
  # When preview button is selected, the chemical list csv file is read in according to the specified delimiter 
  # The indicated number of rows will be skipped such that the first chemical appears in row 1
  # The variable in column A will be called DTXSID
  # A datatable will then be generated to allow the user to preview the upload
  uploaded_data2 <- eventReactive(input$preview_button2, {
    req(input$file2)
    inFile <- input$file2
    delimiter <- ifelse(input$delim2 == "", ",", input$delim2)
    data <- read.csv(inFile$datapath, sep = delimiter, header = FALSE, skip = input$skip2)
    names(data)[1] <- "DTXSID"
    return(data)
  })
  
  output$preview2 <- renderDT({
    if(input$preview_button2){
      uploaded_data2()
    }
  })
  
  # A function to connect to res_mmdb and subset the data according to the selections by the user
  # A datatable will be returned
  generate_data2 <- function(data_type) {
    
    # Connect to the database
    con <- connect_to_mmdb()
    dbGetQuery(con, "set search_path to mmdb")
    
    # Collect relevant information from substances, files, source, and media tables
    chems <- tbl(con, "substances") %>% 
      select(-c(external_id, created_dt:record_exp_dt)) %>% 
      collect()
    
    files <- tbl(con, "files") %>% 
      select(-c(location, filename, mapped:record_exp_dt)) %>% 
      collect()
    
    source <- tbl(con, "source") %>% 
      select(-c(data_collection_type, oppt_phase:loaded, created_dt:record_exp_dt)) %>% 
      collect()
    
    media <- tbl(con, "media") %>% 
      select(-c(created_dt:record_exp_dt)) %>% 
      collect()
    
    # Join the files and source tables by source_id - to be used later
    join <- left_join(files, source, by = "source_id")
    
    # Create a new variable to reflect the chemicals the user uploads, which will be used to subset the data
    DTXSIDs <- uploaded_data2()
    
    chemicallist_chems_in_database <- chems[which(chems$dtxsid %in% DTXSIDs$DTXSID),]
    
    # Conditional statement to reflect whether to search 'aggregate' or 'single-sample' (also known as raw) data
    if(data_type == "Aggregate") {
      
      # Gather data from harmonized_aggregate table
      # Filter data based on user inputs for chemical list
      # Exclude variables created_dt through record_exp_dt because not useful information
      aggregate_data <- tbl(con, "harmonized_aggregate") %>% 
        filter(substance_id %in% !!chemicallist_chems_in_database$substance_id) %>%
        select(-c(created_dt:record_exp_dt)) %>% 
        collect()
      
      # Add relevant variables from files, source, media, and chems tables using join statements
      # Set output to data frame
      # Set order in which variables will be displayed
      # Filter out designated list of chemicals
      aggregate_data <- left_join(aggregate_data, join) %>%
        left_join(., media[,c("media_id", "harmonized_medium")], by = "media_id") %>%
        left_join(., chems[,c("substance_id", "dtxsid", "preferred_name", "casrn", "howcurated", "dtxrid", "substance_type", "structure_inchikey", "structure_formula")], by = "substance_id") %>%
        as.data.frame() %>%
        select(substance_id, dtxsid, preferred_name, casrn, reported_chemical_name, reported_casrn, everything())
      
      # If 'return summary information' is selected, the data
      if(input$summary) {
        
        # Set output to datatable for summary calculations
        # Calculate n by grouping as indicated
        aggregate_data <- data.table(aggregate_data)
        
        aggregate_data <- as.data.frame(aggregate_data[, j = list(N_records = .N),
                                                       by = c("substance_id", "full_source_name", "source_name_abbr",
                                                              "reported_media", "reported_species", "harmonized_medium")])
        
        # Add back chemical information after grouping and calculating n
        # Filter out records with unknown media
        # Set which variables to appear in final results
        aggregate_data <- left_join(aggregate_data, chems) %>%
          filter(harmonized_medium != "unknown") %>%
          select(dtxsid, preferred_name, reported_chemical_name, casrn, reported_media, reported_species, harmonized_medium, N_records, full_source_name)
      }
      
      dbDisconnect(con)
      
      return(aggregate_data)
      
    } else {
      
      # Gather data from raw_aggregate table
      # Filter data based on user inputs for chemical list
      # Exclude variables created_dt through record_exp_dt because not useful information
      raw_data <- tbl(con, "harmonized_raw") %>% 
        filter(substance_id %in% !!chemicallist_chems_in_database$substance_id) %>%
        select(-c(created_dt:record_exp_dt)) %>% 
        collect()
      
      # Add relevant variables from files, source, media, and chems tables using join statements
      # Set output to data frame
      # Set order in which variables will be displayed
      # Filter out designated list of chemicals
      raw_data <- left_join(raw_data, join) %>%
        left_join(., media[,c("media_id", "harmonized_medium")], by = "media_id") %>%
        left_join(., chems[,c("substance_id", "dtxsid", "preferred_name", "casrn", "howcurated", "dtxrid", "substance_type", "structure_inchikey", "structure_formula")], by = "substance_id") %>%
        as.data.frame() %>%
        select(substance_id, dtxsid, preferred_name, casrn, reported_chemical_name, reported_casrn, everything())
      
      # If 'return summary information' is selected, the data 
      if(input$summary) {
        
        # Set output to datatable for summary calculations
        # Calculate n by grouping as indicated
        raw_data <- data.table(raw_data)
        
        raw_data <- as.data.frame(raw_data[, j = list(N_records = .N),
                                           by = c("substance_id", "full_source_name", "source_name_abbr",
                                                  "reported_media", "reported_species", "harmonized_medium")])
        
        # Add back chemical information after grouping and calculating n
        # Filter out records with unknown media
        # Set which variables to appear in final results
        raw_data <- left_join(raw_data, chems) %>%
          filter(harmonized_medium != "unknown") %>%
          select(dtxsid, preferred_name, reported_chemical_name, casrn, reported_media, reported_species, harmonized_medium, N_records, full_source_name)
      }
      
      dbDisconnect(con)
      
      return(raw_data)
      
    }
  }
  
  # When 'download results' is selected, a csv file will be downloaded locally with a title of "MMDB_download_by_DTXSID_type_Aggregate" or "MMDB_download_by_DTXSID_type_Single-sample"
  # While the file is downloading, a message will appear; once the download is complete, the message will disappear
  output$download2 <- downloadHandler(
    filename = function() {
      paste("MMDB_download_by_DTXSID_type_", gsub(" ", "_", input$type2), ".csv", sep = "")
    },
    
    content = function(filename) {
      showModal(modalDialog("Downloading... This may take several minutes", footer = NULL))
      on.exit(removeModal())
      
      data <- generate_data2(input$type2)
      write.csv(x = data, file = filename, row.names = FALSE)
    })
  
## Query by Media Sub-Tab
  
  # A function to connect to res_mmdb and subset the data according to the selections by the user
  # A datatable will be returned
  generate_data3 <- function(data_type) {
    
    # Connect to the database
    con <- connect_to_mmdb()
    dbGetQuery(con, "set search_path to mmdb")
    
    # Collect relevant information from substances, files, source, and media tables
    chems <- tbl(con, "substances") %>% 
      select(-c(external_id, created_dt:record_exp_dt)) %>% 
      collect()
    
    files <- tbl(con, "files") %>% 
      select(-c(location, filename, mapped:record_exp_dt)) %>% 
      collect()
    
    source <- tbl(con, "source") %>% 
      select(-c(data_collection_type, oppt_phase:loaded, created_dt:record_exp_dt)) %>% 
      collect()
    
    media <- tbl(con, "media") %>% 
      select(-c(created_dt:record_exp_dt)) %>% 
      collect()
    
    # Create a new variable to reflect the media types the user selects, which will be used to subset the data
    selected_media <- media[which(media$harmonized_medium %in% c(input$media3, input$media4)),]
    
    # Join the files and source tables by source_id - to be used later
    join <- left_join(files, source, by = "source_id")
    
    # Conditional statement to reflect whether to search 'aggregate' or 'single-sample' (also known as raw) data
    if(data_type == "Aggregate") {
      
      # Gather data from harmonized_aggregate table
      # Exclude variables created_dt through record_exp_dt because not useful information
      # Filter data based on user inputs for media type
      # Filter data to only show records where detected
      aggregate_data <- tbl(con, "harmonized_aggregate") %>% 
        select(-c(created_dt:record_exp_dt)) %>% 
        filter(media_id %in% !!selected_media$media_id) %>%
        filter(detected > 0) %>%
        collect()
      
      # Add relevant variables from files, source, media, and chems tables using join statements
      # Set which variables to appear in final results
      # Filter out results with unknown DTXSIDs and unknown media
      # Return only unique records
      # Filter out designated list of chemicals
      aggregate_data <- left_join(aggregate_data, join) %>%
        left_join(., media[,c("media_id", "harmonized_medium")], by = "media_id") %>%
        left_join(., chems[,c("substance_id", "dtxsid", "preferred_name", "casrn", "howcurated", "dtxrid", "substance_type", "structure_inchikey", "structure_formula")], by = "substance_id") %>%
        select(dtxsid, harmonized_medium, preferred_name) %>%
        filter(!is.na(dtxsid)) %>%
        filter(!is.na(harmonized_medium)) %>%
        distinct() 
      
      dbDisconnect(con)
      
      return(aggregate_data)
      
    } else {
      
      # Gather data from raw_aggregate table
      # Exclude variables created_dt through record_exp_dt because not useful information
      # Filter data based on user inputs for media type
      # Filter data to only show records where detected
      raw_data <- tbl(con, "harmonized_raw") %>% 
        select(-c(created_dt:record_exp_dt)) %>% 
        filter(media_id %in% !!selected_media$media_id) %>%
        filter(detected > 0) %>%
        collect()
      
      # Add relevant variables from files, source, media, and chems tables using join statements
      # Set which variables to appear in final results
      # Filter out results with unknown DTXSIDs and unknown media
      # Return only unique records
      # Filter out designated list of chemicals
      raw_data <- left_join(raw_data, join) %>%
        left_join(., media[,c("media_id", "harmonized_medium")], by = "media_id") %>%
        left_join(., chems[,c("substance_id", "dtxsid", "preferred_name", "casrn", "howcurated", "dtxrid", "substance_type", "structure_inchikey", "structure_formula")], by = "substance_id") %>%
        select(dtxsid, harmonized_medium, preferred_name) %>%
        filter(!is.na(dtxsid)) %>%
        filter(!is.na(harmonized_medium)) %>%
        distinct() 
      
      dbDisconnect(con)
      
      return(raw_data)
      
    }
  }
  
  # When 'download results' is selected, a csv file will be downloaded locally with a title of "MMDB_download_by_media_type_Aggregate" or "MMDB_download_by_media_type_Single-sample"
  # While the file is downloading, a message will appear; once the download is complete, the message will disappear
  output$download3 <- downloadHandler(
    filename = function() {
      paste("MMDB_download_by_media_type_", gsub(" ", "_", input$type3), ".csv", sep = "")
    },
    
    content = function(filename) {
      showModal(modalDialog("Downloading... This may take several minutes", footer = NULL))
      on.exit(removeModal())
      
      data <- generate_data3(input$type3)
      write.csv(x = data, file = filename, row.names = FALSE)
    })
  
## Metadata Tab          
  
  # A function to download the metadata from online & read it in as a datatable
  # Requires url input and number of rows to skip in order to reach the first row of data
  getDataTable <- function(url, skip) {
    download.file(url, "data.xlsx", mode = "wb")
    data <- read_excel("data.xlsx", skip = skip)
    datatable(data)
  }
  
  # Generate datatable of variable mapping info; skip first 7 rows of raw data
  observeEvent(input$view_var_map, {
    output$preview_var_map <- renderDT({
      getDataTable(var_map_url, 7)
    })
  })
  
  # Generate datatable of variable info; skip first 8 rows of raw data
  observeEvent(input$view_vars, {
    output$preview_vars <- renderDT({
      getDataTable(vars_url, 8)
    })
  })
  
  # Generate datatable of source info; skip first 13 rows of raw data
  observeEvent(input$view_source, {
    output$preview_source <- renderDT({
      getDataTable(source_url, 13)
    })
  })
  
  # Generate datatable of media mapping info; skip first 8 rows of raw data 
  observeEvent(input$view_media_map, {
    output$preview_media_map <- renderDT({
      getDataTable(media_map_url, 8)
    })
  })
  
  # Generate datatable of chemical mapping info; skip first 15 rows of raw data
  observeEvent(input$view_chem_map, {
    output$preview_chem_map <- renderDT({
      getDataTable(chem_map_url, 15)
    })
  })
}

# Run the application 
shinyApp(ui = ui, server = server)
