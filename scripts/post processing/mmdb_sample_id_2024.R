## Assigns a standardized sample ID to the harmonized raw data table in the res_mmdb database.
## Authors: Rachel Hencher
## Created: 2024-05-21
## Last updated: 2024-08-09

message("Running script to generate mmdb_sample_id variable at: ", Sys.time())

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

#'@description A function to mutate input dataframe by the specified conditions.
#'@param .data An input dataframe to mutate.
#'@param condition A boolean statement to filter the dataframe to.
#'@param ... Additional mutate parameters.
#'@return Modified input dataframe filtered to the condition parameter.
mutate_cond <- function(.data, condition, ..., envir = parent.frame()) {
  condition <- eval(substitute(condition), .data, envir)
  .data[condition, ] <- .data[condition, ] %>% mutate(...)
  .data
}

#'@description A function to get the database source table with data source specific information.
#'@return A dataframe with data source information.
get_sources <- function(){
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  sources = tbl(conn, "source") %>% collect()
  dbDisconnect(conn)
  return(sources)
}

#'@description A function to get the data source ID from the source_name parameter.
#'@param source_name A string of the data source's abbreviated name (e.g. ahhs, usgs).
#'@return An integer value for the data source's ID value.
getsourceid <- function(source_name) {
  return(get_sources() %>%
           filter(source_name_abbr == source_name) %>%
           select(source_id))
}

############################################################
# Primary Functions
############################################################

#'@description A function to assign the mmdb_sample_id variable for a 'single-sample' data source in the database.
#'@return None. PostgreSQL queries are pushed to the database.
assign.raw <- function() {
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  # Get source table information.
  sources <- tbl(conn, "source") %>% collect()
  
  # Get the harmonized aggregate data information for the selected variables.
  message("Pulling single-sample data for all sources but USGS at: ", Sys.time())
  
  harm_raw <- tbl(conn, "harmonized_raw") %>% 
    filter(source_id != 168) %>%
    select(
      harmonized_raw_id, source_id, reported_sample_id, media_id, reported_collection_activity_id, 
      sample_month, sample_year, reported_dates, reported_location, sample_time, state_or_province
    ) %>%
    collect()
  
  # Add a new variable to the harmonized raw data table for standardized sample.
  harm_raw$temp_sample_id <- NA_integer_
  harm_raw$temp_sample_id2 <- NA_character_
  harm_raw$mmdb_sample_id <- NA_character_

  # Add standardized sample IDs to each aggregate data source.
  message("Assigning standardized sample IDs to single-sample data at: ", Sys.time())

  ## Begin: chem_theatre
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("chem_theatre", getsourceid),
      # Utilizes reported_sample_id to identify a sample
      temp_sample_id = as.numeric(as.factor(reported_sample_id)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: chem_theatre
  
  
  ## Begin: epa_9potw
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("epa_9potw", getsourceid),
      # Unique combinations of reported_dates & media_id identify a sample
      temp_sample_id2 = paste0(reported_dates, media_id),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: epa_9potw
  
  
  ## Begin: ca_surf_sediment
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("ca_surf_sediment", getsourceid),
      # Unique combinations of reported_dates, sample_time & reported_location identify a sample
      temp_sample_id2 = paste0(reported_dates, reported_location, sample_time),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ca_surf_sediment
  
  
  ## Begin: ca_surf_water
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("ca_surf_water", getsourceid),
      # Unique combinations of reported_dates & reported_location identify a sample
      temp_sample_id2 = paste0(reported_dates, reported_location),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ca_surf_water
  
  
  ## Begin: epa_amtic
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("epa_amtic", getsourceid),
      # Unique combinations of reported_dates, reported_collection_activity_id & state identify a sample
      temp_sample_id2 = paste0(reported_dates, reported_collection_activity_id, state_or_province),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: epa_amtic
  
  
  ## Begin: epa_tnsss
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("epa_tnsss", getsourceid),
      # Utilizes reported_sample_id to identify a sample
      temp_sample_id = as.numeric(as.factor(reported_sample_id)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: epa_tnsss
  
  
  ## Begin: epa_ucmr
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("epa_ucmr", getsourceid),
      # First utilizes reported_sample_id to identify a sample
      # If no reported_sample_id is provided, unique combinations of reported_dates & reported_location identify the sample
      temp_sample_id2 = case_when(
        is.na(reported_sample_id) ~ paste0(reported_dates, reported_location),
        TRUE ~ reported_sample_id
      ),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: epa_ucmr
  
  
  ## Begin: fda_tds_elem
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("fda_tds_elem", getsourceid),
      # Unique combinations of reported_collection_activity_id & media_id identify a sample
      temp_sample_id2 = paste0(reported_collection_activity_id, media_id),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: fda_tds_elem
  
  
  ## Begin: fda_tds_pest
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("fda_tds_pest", getsourceid),
      mmdb_sample_id = NA_character_
    )
  ## End: fda_tds_pest
  
  
  ## Begin: ices_biota
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("ices_biota", getsourceid),
      # Utilizes reported_sample_id to identify a sample
      temp_sample_id = as.numeric(as.factor(reported_sample_id)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ices_biota
  
  
  ## Begin: ices_sediment
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("ices_sediment", getsourceid),
      # Utilizes reported_sample_id to identify a sample
      temp_sample_id = as.numeric(as.factor(reported_sample_id)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ices_sediment
  
  
  ## Begin: airmon
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("airmon", getsourceid),
      # Unique combinations of reported_dates & reported_location identify a sample
      # If no reported_date is provided, the sample cannot be identified
      temp_sample_id2 = paste0(reported_dates, reported_location),
      temp_sample_id = case_when(
        is.na(reported_dates) ~ NA_integer_,
        TRUE ~ as.numeric(as.factor(temp_sample_id2))
      ),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: airmon
  
  
  ## Begin: ip_chem_biota
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("ip_chem_biota", getsourceid),
      # First utilizes reported_sample_id to identify a sample
      # If no reported_sample_id is provided (or if sample ID is <= 0), unique combinations of sample_month, sample_year, & reported_location identify the sample
      temp_sample_id2 = case_when(
        reported_sample_id <= 0 ~ paste0(sample_month, sample_year, reported_location),
        reported_sample_id == "null" ~ paste0(sample_month, sample_year, reported_location),
        reported_sample_id == "X" ~ paste0(sample_month, sample_year, reported_location),
        is.na(reported_sample_id) ~ paste0(sample_month, sample_year, reported_location),
        TRUE ~ reported_sample_id
      ),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ip_chem_biota
  
  
  ## Begin: ip_chem_seawater
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("ip_chem_seawater", getsourceid),
      # If no reported_sample_id is provided, unique combinations of sample_month, sample_year, & reported_location identify the sample
      # If reported_sample_id is provided, utilizes reported_sample_id in conjunction with date and location info to identify a sample
      temp_sample_id2 = case_when(
        is.na(reported_sample_id) ~ paste0(sample_month, sample_year, reported_location),
        reported_sample_id == 0 ~ paste0(sample_month, sample_year, reported_location),
        TRUE ~ paste0(reported_sample_id, sample_month, sample_year, reported_location)
      ),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ip_chem_seawater
  
  
  ## Begin: ip_chem_sediment
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("ip_chem_sediment", getsourceid),
      # First utilizes reported_sample_id to identify a sample
      # If no reported_sample_id is provided, unique combinations of sample_month, sample_year, & reported_location identify the sample
      temp_sample_id2 = case_when(
        is.na(reported_sample_id) ~ paste0(sample_month, sample_year, reported_location),
        reported_sample_id == "null" ~ paste0(sample_month, sample_year, reported_location),
        reported_sample_id == 0 ~ paste0(sample_month, sample_year, reported_location),
        TRUE ~ paste0(sample_month, sample_year, reported_location, reported_sample_id)
      ),
      temp_sample_id = as.numeric(as.factor(temp_sample_id2)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ip_chem_sediment
  
  
  ## Begin: usda_nrp
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("usda_nrp", getsourceid),
      # Utilizes reported_sample_id to identify a sample
      temp_sample_id = as.numeric(as.factor(reported_sample_id)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: usda_nrp
  
  
  ## Begin: ip_chem_ibs
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("ip_chem_ibs", getsourceid),
      # Utilizes reported_sample_id to identify a sample
      temp_sample_id = as.numeric(as.factor(reported_sample_id)),
      mmdb_sample_id = paste0(source_id, sep = "_", temp_sample_id)
    )
  ## End: ip_chem_ibs
  
  # Pushes data into database.
  message("Writing standardized sample IDs to harmonized raw table at: ", Sys.time())
  
  harm_raw_update <- harm_raw %>%
    select(harmonized_raw_id, mmdb_sample_id)
  
  dbWriteTable(conn = conn, name = "temp_harm_raw_update", value = harm_raw_update, row.names = F)
  
  query <- "UPDATE harmonized_raw
  SET mmdb_sample_id = temp_harm_raw_update.mmdb_sample_id
  FROM temp_harm_raw_update
  WHERE harmonized_raw.harmonized_raw_id = temp_harm_raw_update.harmonized_raw_id"
  dbSendStatement(conn, query)
  rm(query)
  
  query <- "DROP TABLE temp_harm_raw_update"
  dbSendStatement(conn, query)
  rm(query)
  
  dbDisconnect(conn)
}

#'@description A function to assign the mmdb_sample_id variable for USGS data in the database.
#'@return None. PostgreSQL queries are pushed to the database.
assign.usgs <- function() {
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  # Get the harmonized aggregate data information for the selected variables.
  message("Pulling data for USGS...", Sys.time())
  
  harm_usgs <- tbl(conn, "harmonized_raw") %>% 
    filter(source_id == 168) %>%
    select(
      harmonized_raw_id, source_id, reported_sample_id, 
      reported_collection_activity_id, reported_dates, reported_location 
    ) %>%
    collect()
  
  message("Assigning standardized sample IDs to USGS data...", Sys.time())
  
  # Add a new variable to the harmonized raw data table for standardized sample.
  harm_usgs$temp_sample_id2 <- paste0(harm_usgs$reported_collection_activity_id, harm_usgs$reported_dates, harm_usgs$reported_location)
  harm_usgs$temp_sample_id <- as.numeric(as.factor(harm_usgs$temp_sample_id2))
  harm_usgs$mmdb_sample_id <- paste0(harm_usgs$source_id, sep = "_", harm_usgs$temp_sample_id)
  
  # Pushes data into database.
  message("Writing USGS standardized sample IDs to harmonized raw table...", Sys.time())
  
  harm_usgs_update <- harm_usgs %>%
    select(harmonized_raw_id, mmdb_sample_id)
  
  dbWriteTable(conn = conn, name = "temp_harm_usgs_update", value = harm_usgs_update, row.names = F)
  
  query <- "UPDATE harmonized_raw
  SET mmdb_sample_id = temp_harm_usgs_update.mmdb_sample_id
  FROM temp_harm_usgs_update
  WHERE harmonized_raw.harmonized_raw_id = temp_harm_usgs_update.harmonized_raw_id"
  dbSendStatement(conn, query)
  rm(query)
  
  query <- "DROP TABLE temp_harm_usgs_update"
  dbSendStatement(conn, query)
  rm(query)
  
  dbDisconnect(conn)
}

#'@description A function to run each of the sample ID assignment functions.
#'@return None. RPostgreSQL queries are pushed to the database.
assign.sampleID <- function() {
  assign.raw()
  #assign.usgs()
}

# Run the assignment function to run workflow. Remember to uncomment out the actual database push query lines.
assign.sampleID()