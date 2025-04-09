## Assigns a cleaned_units variable, where applicable, to the harmonized aggregate and raw data tables in the res_mmdb database.
## Authors: Rachel Hencher
## Created: 2024-10-15
## Last updated: 2024-10-16

message("Running script to generate cleaned_units variable at: ", Sys.time())

# Load relevant packages
required.packages = c('DBI', 'dplyr', 'dbplyr', 'data.table')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

############################################################
# Helper Functions
############################################################

#'@description A function to create a PostgreSQL database connection from .Renviron parameters.
#'@return RPostgreSQL connection.
connect_to_db = function(){
  return(dbConnect(RPostgreSQL::PostgreSQL(), 
                   user = Sys.getenv("postgres_user"), 
                   password = Sys.getenv("postgres_pass"),
                   host = Sys.getenv("postgres_host"),
                   dbname = Sys.getenv("postgres_dbname")))
}

############################################################
# Primary Functions
############################################################

#'@description A function to assign the cleaned_units variable for an 'aggregate' data source in the database.
#'@return None. PostgreSQL queries are pushed to the database.
assign.aggregate <- function(cleaned_units_file) {
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")


  # Get the harmonized aggregate data information for the selected variables.
  message("Pulling aggregate data for all sources at: ", Sys.time())
  
  harm_agg <- tbl(conn, "harmonized_aggregate") %>% 
    select(
      harmonized_aggregate_id, source_id, reported_units
    ) %>%
    collect()
  
  # Add cleaned units to each aggregate data source
  message("Assigning cleaned units to aggregate data at: ", Sys.time())

  harm_agg <- harm_agg %>% 
    left_join(., cleaned_units_file, by = c("source_id", "reported_units"))
  
  # Pushes data into database
  message("Writing cleaned units to harmonized aggregate table at: ", Sys.time())
  
  harm_agg_update <- harm_agg %>%
    select(harmonized_aggregate_id, cleaned_units)
  
  dbWriteTable(conn = conn, name = "temp_harm_agg_update", value = harm_agg_update, row.names = F)
  
  query <- "UPDATE harmonized_aggregate
  SET cleaned_units = temp_harm_agg_update.cleaned_units
  FROM temp_harm_agg_update
  WHERE harmonized_aggregate.harmonized_aggregate_id = temp_harm_agg_update.harmonized_aggregate_id"
  dbSendStatement(conn, query)
  rm(query)
  
  query <- "DROP TABLE temp_harm_agg_update"
  dbSendStatement(conn, query)
  rm(query)
  
  dbDisconnect(conn)
}


#'@description A function to assign the cleaned_units variable for a 'raw' data source in the database.
#'@return None. PostgreSQL queries are pushed to the database.
assign.raw <- function(cleaned_units_file) {
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  
  # Get the harmonized raw data information for the selected variables.
  message("Pulling single-sample data for all sources, excluding USGS, at: ", Sys.time())
  
  harm_raw <- tbl(conn, "harmonized_raw") %>% 
    filter(source_id != 168) %>%
    select(
      harmonized_raw_id, source_id, reported_units
    ) %>%
    collect()
  
  # Add cleaned units to each raw data source
  message("Assigning cleaned units to single-sample data at: ", Sys.time())
  
  harm_raw <- harm_raw %>% 
    left_join(., cleaned_units_file, by = c("source_id", "reported_units"))
  
  # Pushes data into database
  message("Writing cleaned units to harmonized raw table at: ", Sys.time())
  
  harm_raw_update <- harm_raw %>%
    select(harmonized_raw_id, cleaned_units)
  
  dbWriteTable(conn = conn, name = "temp_harm_raw_update", value = harm_raw_update, row.names = F)
  
  query <- "UPDATE harmonized_raw
  SET cleaned_units = temp_harm_raw_update.cleaned_units
  FROM temp_harm_raw_update
  WHERE harmonized_raw.harmonized_raw_id = temp_harm_raw_update.harmonized_raw_id"
  dbSendStatement(conn, query)
  rm(query)
  
  query <- "DROP TABLE temp_harm_raw_update"
  dbSendStatement(conn, query)
  rm(query)
  
  dbDisconnect(conn)
}

#'@description A function to assign the detection flag (0-1) for the USGS data source in the database. This source is so large,
#'it requires a special approach so the SQL queries work faster & R doesn't become memory overloaded.
#'@return None. MySQL queries are pushed to the database.
assign.usgs <- function() {
  # We have to pull USGS data one file at a time, because if we try to pull all files at once, it will be too big to collect into a tibble.
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  fileIDs <- tbl(conn, "files") %>% filter(source_id == 168) %>% select(file_id) %>% collect()
  fileIDs <- fileIDs$file_id
  
  for(f in fileIDs) {
    message("Pulling data for usgs file ", f, " at: ", Sys.time())
    usgs <- tbl(conn, "harmonized_raw") %>%
      select(
        harmonized_raw_id, source_id, file_id, reported_units
      ) %>%
      filter(source_id == 168, file_id == f) %>%
      collect()
    
    # Add cleaned units to USGS source
    message("Assigning cleaned units to usgs file ", f, " at: ", Sys.time())
    
    usgs <- usgs %>% 
      left_join(., cleaned_units_file, by = c("source_id", "reported_units"))

    
    # Pushes data into database
    usgs_update <- usgs %>%
      select(harmonized_raw_id, cleaned_units)
    
    dbWriteTable(conn = conn, name = "temp_usgs_update", value = usgs_update, row.names = F)
    
    query <- "UPDATE harmonized_raw
    SET cleaned_units = temp_usgs_update.cleaned_units
    FROM temp_usgs_update
    WHERE harmonized_raw.harmonized_raw_id = temp_usgs_update.harmonized_raw_id"
    dbSendStatement(conn, query)
    rm(query)
    
    query <- "DROP TABLE temp_usgs_update"
    dbSendStatement(conn, query)
    rm(query)
  }
  dbDisconnect(conn)
}


#'@description A function to run each of the cleaned unit assignment functions.
#'@return None. RPostgreSQL queries are pushed to the database.
assign.cleaned_units_var <- function() {
  #assign.aggregate()
  #assign.raw()
  assign.usgs()
}


# Read in cleaned unit information from csv
cleaned_units_file <- read.csv("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/cleaned units/units_2024-10-15.csv")

# Run the assignment function to run workflow. Remember to uncomment out the actual database push query lines.
assign.cleaned_units_var()