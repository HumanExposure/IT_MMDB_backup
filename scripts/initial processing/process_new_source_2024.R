## Script to load data in MMDB raw data tables into harmonized tables using mapped field names.
## Author: Jonathan Taylor Wall & Rachel Hencher
## Created: 2021-05-28
## Last updated: 2024-07-25

message("Running script to process data at: ", Sys.time())
required.packages = c('DBI', 'plyr', 'dbplyr', 'reshape', 'magrittr', 'tidyr')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

######################################################################
# Functions
######################################################################

#'@description A function to make a connection to the MMDB database.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import DBI RPostgreSQL RMySQL RSQLite
#'@return Database connection pointer object.
connect_to_mmdb <- function(con_type){
  switch(con_type,
         "postgres" = dbConnect(RPostgreSQL::PostgreSQL(), 
                                user = Sys.getenv("postgres_user"), 
                                password = Sys.getenv("postgres_pass"),
                                host = Sys.getenv("postgres_host"),
                                dbname = Sys.getenv("postgres_dbname")),
         "mysql" = dbConnect(RMySQL::MySQL(),
                             username = Sys.getenv("mysql_user"), 
                             password = Sys.getenv("mysql_pass"),
                             host = Sys.getenv("mysql_host"), 
                             port = as.integer(Sys.getenv("mysql_port")),
                             dbname = Sys.getenv("mysql_dbname")),
         'sqlite' = dbConnect(RSQLite::SQLite(), "prod_samples.sqlite")
  ) %>% return()
}

#'@description A helper function to query MMDB & receive the results. 
#'Handles errors/warnings with tryCatch.
#'@param query A SQL query string to query the database with.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import DBI dplyr magrittr
#'@return Dataframe of database query results.
query_mmdb <- function(query = NULL, con_type){
  if(is.null(query)) return(message("Must provide a query to send."))
  
  con = connect_to_mmdb(con_type)
  query_result = tryCatch({
    if(con_type == "postgres"){
      return(dbGetQuery(con, query %>% gsub("FROM ", "FROM mmdb.", .)))
    } else {
      return(dbGetQuery(con, query))
    }
  },
  error = function(cond){ message("Error message: ", cond); return(NULL) },
  finally = { dbDisconnect(con) })
  return(query_result)
}

#'@description A helper function to send a statement to MMDB to update table entries. 
#'Handles errors/warnings with tryCatch.
#'@param statement A SQL query string to send to the database.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import DBI dplyr magrittr
#'@return None. A SQL statement is passed to the database to make changes to the database.
send_statement_mmdb <- function(statement = NULL, con_type = NULL){
  if(is.null(statement)) return(message("Must provide a statement to send."))
  
  con = connect_to_mmdb(con_type = con_type)
  tryCatch({
    if(con_type == "postgres"){
      return(dbSendStatement(con, statement %>%
                               gsub("FROM ", "FROM mmdb.", .) %>%
                               gsub("INTO ", "INTO mmdb.", .) %>%
                               gsub("UPDATE ", "UPDATE mmdb.", .) %>%
                               gsub("EXISTS ", "EXISTS mmdb.", .)
      ) %>% dbClearResult())
    } else {
      return(dbSendStatement(con, statement) %>% dbClearResult())
    }
  },
  error = function(cond){ message("Error message: ", cond); return(NA) },
  warning = function(cond){ message("Warning message: ", cond); return(NULL) },
  finally = { dbDisconnect(con) })
}

#'@description A helper function to write a table to the MMDB database. Note, if it already exists, 
#'the table will be overwritten.
#'@param name The name of the table to write to the database.
#'@param data The data to push into the new database table.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import DBI
#'@return None. A table is written (or overwritten) on the database with a given 'name' and 'data'.
write_table_mmdb <- function(name = NULL, data = NULL, con_type = NULL){
  if(is.null(name)) return(message("Must provide a name for the database table."))
  if(is.null(data)) return(message("Must provide data to push to the database table."))
  
  con = connect_to_mmdb(con_type)
  tryCatch({
    if(con_type == "postgres"){
      dbWriteTable(con, name = c('mmdb', name), value = data, row.names = F, overwrite = T)
    } else {
      dbWriteTable(con, name = name, value = data, row.names = F, overwrite = T)
    }
  },
  error = function(cond){ message("Error message: ", cond); return(NA) },
  warning = function(cond){ message("Warning message: ", cond); return(NULL) },
  finally = { dbDisconnect(con) })
}

#'@description A function to initialize rows in the data source's harmonized table.
#'@param source_id The unique ID for a data source from the MMDB source table.
#'@param source_name A data source's abbreviated name (e.g. ahhs, ctd).
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@return Boolean of whether the source was successfully initialized. SQL statements update the MMDB database.
init_source_harmonized <- function(source_id = NULL, source_name = NULL, con_type = NULL){
  if(is.null(source_id)) return(message("Must provide data the data source's unique ID"))
  if(is.null(source_name)) return(message("Must provide data source abbreviated name"))
  
  message("...Initializing ", source_name, " into harmonized table...")
  # Get data source ID.
  if(nrow(source_id) != 1){ 
    message("Source ", source_name, " does not have a source_id or has duplicates... Cannot process... Skipping...")
    return(FALSE)
  }
  # Set queries for initializing data.
  if(con_type == "postgres"){
    query = paste0("SELECT data_collection_type FROM source WHERE source_name_abbr = '", source_name, "'")
    query_init = paste0("SELECT harm_init FROM source WHERE source_name_abbr = '", source_name, "'")
    query_file_not_init = paste0("SELECT file_id FROM files WHERE harm_init = FALSE AND source_id = '", source_id, "'")
    query_need_init = paste0("SELECT SUM(CASE WHEN harm_init = FALSE THEN 1 ELSE 0 END) needinitcount ",
                             "FROM files WHERE source_id = '", source_id, "'")
    query_update_source_init = paste0("UPDATE source SET harm_init = TRUE WHERE source_id ='", source_id, "'")
  } else {
    query = paste0("SELECT type FROM source WHERE name = '", source_name, "'")
    query_init = paste0("SELECT harm_init FROM source WHERE name = '", source_name, "'")
    query_file_not_init = paste0("SELECT fileid FROM files WHERE harm_init = 0 AND source_sourceid = '", source_id, "'")
    query_need_init = paste0("SELECT SUM(CASE WHEN harm_init = 0 THEN 1 ELSE 0 END) needinitcount ",
                             "FROM files WHERE source_sourceid = '", source_id, "'")
    query_update_source_init = paste0("UPDATE source SET harm_init = 1 WHERE sourceid ='", source_id, "'")
  }
  # Get data source type.
  type = query_mmdb(query, con_type)
  # Get initialization status (have harmonized table rows been templated for the source(s)?).
  fk_initialized = query_mmdb(query_init, con_type)
  
  # Check if any files are not initialized from the source's raw table and into its harmonized table. 
  # If any found, initialize them. Initialization just adds default/empty rows to be mapped/filled.
  if (fk_initialized$harm_init == 0) {
    message("Initializing source ", source_name)
    # Get list of files not already initialized into the harmonized table.
    fileid_not_init = query_mmdb(query_file_not_init, con_type)
    # Handling case where for some reason harm_init is 0 but all files are initialized...
    if(nrow(fileid_not_init)){
      # Initialize data records for this source, loop through each file individually.
      for(f in fileid_not_init$file_id){
        if(con_type == "postgres"){
          query_raw_data = paste0("SELECT DISTINCT file_id, source_id, record_id FROM ", 
                                  "raw_", ifelse(type == "raw", "", "aggregate_"), "data",
                                  " WHERE file_id IN (", 
                                  paste(f, collapse=", "),") AND source_id in ('", 
                                  source_id, "')")
          query_harmonized_init = paste0("INSERT INTO ", paste0("harmonized_", type),"  
                                  (file_id, source_id, record_id, media_id, substance_id) 
                                  SELECT file_id, source_id, record_id, media_idmedia, substances_idsubstance 
                                  FROM temp_table2")
          query_change_init = paste0("UPDATE files SET harm_init = TRUE WHERE file_id IN (",
                                     paste(f, collapse =", "),") AND source_id ='",
                                     source_id, "'")
        } else {
          query_raw_data = paste0("SELECT DISTINCT files_fileid, files_source_sourceid, record_id FROM ", 
                                  "raw_", ifelse(type == "raw", "", "aggregate_"), "data",
                                  " WHERE files_fileid IN (", 
                                  paste(f, collapse =", "),") AND files_source_sourceid in ('", 
                                  source_id, "')")
          query_harmonized_init = paste0("INSERT INTO ", paste0("harmonized_", type),"  
                                  (files_fileid, files_source_sourceid, record_id, media_idmedia, substances_idsubstance) 
                                  SELECT files_fileid, files_source_sourceid, record_id, media_idmedia, substances_idsubstance 
                                  FROM temp_table2")
          query_change_init = paste0("UPDATE files SET harm_init = 1 WHERE files.fileid IN (",
                                     paste(f, collapse =", "),") AND files.source_sourceid ='",
                                     source_id, "'")
        }
        # Pull raw data for the file.
        init = query_mmdb(query_raw_data, con_type)
        # Set default media and substance ID values for the harmonized table.
        init$media_idmedia <- 99999
        init$substances_idsubstance <- 99999
        
        message("...Writing temp_table2 at: ", Sys.time())
        # Generate temporary table temp_table2 & populate with the initialized data.
        write_table_mmdb(name = 'temp_table2', data = init, con_type = con_type)
        
        message("...Writing to harmonized table at: ", Sys.time())
        # Write to harmonized table from temp_table2.
        send_statement_mmdb(query_harmonized_init, con_type = con_type)
        
        message("Done initializing source data at: ", Sys.time())
        # Drop temp_table2.
        send_statement_mmdb("DROP TABLE IF EXISTS temp_table2", con_type = con_type)
        # Update source table to reflect processed status.
        send_statement_mmdb(query_change_init, con_type = con_type)   
      }
    }
    
    # IF statement to only change source harm_init if all files from source are marked as initialized.
    needInit = query_mmdb(query_need_init, con_type = con_type)
    if (!needInit$needinitcount) {
      send_statement_mmdb(query_update_source_init, con_type = con_type)
    }
  } else { 
    message("All files initialized for source: ", source_name, "...")
  }
  
  return(TRUE)
}

#'@description A function to map raw table data to harmonized fields in the harmonized table.
#'@param source_id The unique ID for a data source from the MMDB source table.
#'@param source_map A map of data source fields to harmonize (required source, to, and from columns).
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import dplyr magrittr
#'@return None. SQL statements update the MMDB database.
map_source_harmonized <- function(source_id = NULL, source_map = NULL, con_type = NULL){
  if(is.null(source_id)) return(message("Must provide data the data source's unique ID."))
  if(is.null(source_map)) return(message("Must provide data the data source's variable map."))
  
  # Identify source name & set up queries for mapping.
  source_name = unique(source_map$source)
  if(con_type == "postgres"){
    query_harm_mapped = paste0("SELECT harm_mapped FROM source WHERE source_name_abbr = '", source_name, "'")
    query_type = paste0("SELECT data_collection_type FROM source WHERE source_name_abbr = '", source_name, "'")
    query_file_list = paste0("SELECT DISTINCT(file_id) FROM files WHERE source_id = '", source_id,"' AND mapped = FALSE")
    query_need_map_update = paste0("SELECT SUM(CASE WHEN mapped = FALSE THEN 1 ELSE 0 END) needmapcount 
                                  FROM files WHERE source_id = '", source_id, "'")
    query_need_map = paste0("UPDATE source SET harm_mapped = TRUE WHERE source_id = '", source_id, "'")
  } else {
    query_harm_mapped = paste0("SELECT harm_mapped FROM source WHERE name = '", source_name, "'")
    query_type = paste0("SELECT type FROM source WHERE name = '", source_name, "'")
    query_file_list = paste0("SELECT DISTINCT(fileid) FROM files WHERE source_sourceid ='", source_id, "' AND mapped = 0")
    query_need_map_update = paste0("SELECT SUM(CASE WHEN mapped = 0 THEN 1 ELSE 0 END) needmapcount 
                                  FROM files WHERE source_sourceid = '", source_id, "'")
    query_need_map = paste0("UPDATE source SET harm_mapped = 1 WHERE sourceid = '", source_id, "'")
  }
  # Get mapping status (have harmonized table rows been mapped for the source?).
  fk_mapped = query_mmdb(query_harm_mapped, con_type = con_type)
  # Get source type (single-sample/raw or aggregate).
  type = query_mmdb(query_type, con_type = con_type)
  
  # Check if any initialized files need variables to be mapped. If any found, map file variables.
  # Mapping variables means adding harmonized data from raw data tables using harmonized field names.
  if(!fk_mapped$harm_mapped[1]){
    # Update variables for this source per the mapping (pull all the data down).
    fileIDs = query_mmdb(query_file_list, con_type = con_type)
    # Loop through each file chunk to map.
    for(f in fileIDs[[1]]){ 
      message("Mapping variables for file ID ", f," at: ", Sys.time())
      message(".....Grabbing initialized data at: ", Sys.time())
      if(con_type == "postgres"){
        query_raw_data = paste0("SELECT file_id, record_id, variable_name, variable_value",
                                " FROM raw_", ifelse(type$data_collection_type == "raw", "", "aggregate_"), "data",
                                " WHERE file_id = ", f)
      } else {
        query_raw_data = paste0("SELECT files_fileid, record_id, variable_name, variable_value",
                                " FROM raw_", ifelse(type$type == "raw", "", "aggregate_"), "data",
                                " WHERE files_fileid = ", f)
      }
      
      # Harmonize raw data into harmonized table (large source data vs. smaller source data).
      # Interject USGS fix here for Combining ActivityMediaName where ActivityMediaSubdivisionName is NA.
      if(source_name == "usgs"){
        # Pull all raw data for mapping variables, spread to wide form table, rename columns to mapped names.
        Data <- query_mmdb(query_raw_data, con_type = con_type) %T>% {
          message("...Pulling from raw table at: ", Sys.time()) } %>% 
          collect() %T>% { 
            message("...Rearranging table at:", Sys.time()) } %>%
          mutate_at(vars("variable_value"), list(~trimws(encodeString(.)))) %>%
          # Standardize variable names to remove punctuation, case, and whitespace differences.
          mutate_at(vars("variable_name"), list(~gsub('[[:punct:] ]+',' ', trimws(tolower(.))))) %>%
          filter(variable_name %in% c(!! source_map$from[source_map$source == source_name], "activitymedianame")) %>%
          # Harmonize NA values.
          mutate(variable_value = na_if(x = variable_value, y = c("")),
                 variable_value = na_if(x = variable_value, y = c(" ")),
                 #variable_value = na_if(x = variable_value, y = c("NA")), # Removed for now due to elemental sodium
                 variable_value = na_if(x = variable_value, y = c("<NA>")),
                 variable_value = na_if(x = variable_value, y = c("<na>"))) %>% # Convert certain strings to NA
          spread(key = variable_name, value = variable_value) 
        # Specific fixes for USGS.
        Data$activitymediasubdivisionname[Data$activitymediasubdivisionname == ""] <- NA
        Data$activitymediasubdivisionname[is.na(Data$activitymediasubdivisionname)] <- Data$activitymedianame[is.na(Data$activitymediasubdivisionname)]
        Data <- Data %>%
          select(-activitymedianame) %T>% {
            message("...Renaming mapped variables at: ", Sys.time())
            names(.)[names(.) %in% source_map$from[source_map$source == source_name]] <- left_join(data.frame(from=names(.)[names(.) %in% 
                                                                                                                              source_map$from[source_map$source == source_name]], 
                                                                                                              stringsAsFactors = F), 
                                                                                                   source_map[source_map$source == source_name,], 
                                                                                                   by = "from") %>% 
              select(to) %>% mutate(to = as.character(to)) %>% unlist()
            message("...Returning raw data at: ", Sys.time()) 
          }
      } else {
        # Pull all raw data for mapping variables, spread to wide form table, rename columns to mapped names.
        Data <- query_mmdb(query_raw_data, con_type = con_type) %T>% { 
          message("...Pulling from raw table at: ", Sys.time()) } %T>% { 
            message("...Rearranging table at: ", Sys.time()) } %>%
          mutate_at(vars("variable_value"), list(~trimws(encodeString(.)))) %>%
          # Standardize variable names to remove punctuation, case, and whitespace differences.
          mutate_at(vars("variable_name"), list(~gsub('[[:punct:] ]+', ' ', trimws(tolower(.))))) %>% 
          filter(variable_name %in% !! source_map$from[source_map$source == source_name]) %>%
          # Harmonize NA values.
          mutate(variable_value = na_if(x = variable_value, y = c("")),
                 variable_value = na_if(x = variable_value, y = c(" ")),
                 #variable_value = na_if(x = variable_value, y = c("NA")), # Removed for now due to elemental sodium
                 variable_value = na_if(x = variable_value, y = c("<NA>")),
                 variable_value = na_if(x = variable_value, y = c("<na>"))) %>% # Convert certain strings to NA
          spread(key = variable_name, value = variable_value) %T>% {
            message("...Renaming mapped variables at: ", Sys.time())
            names(.)[names(.) %in% source_map$from[source_map$source == source_name]] <- left_join(data.frame(from = names(.)[names(.) %in% 
                                                                                                                                source_map$from[source_map$source == source_name]], 
                                                                                                              stringsAsFactors = F), 
                                                                                                   source_map[source_map$source == source_name,], by = "from") %>% 
              select(to) %>% mutate(to = as.character(to)) %>% unlist()
            message("...Returning raw data at: ", Sys.time()) 
          }
      } 
      
      # Batch push data to harmonized table.
      batch_map_source_file(df = Data, f = f, type = type, map = source_map, con_type = con_type)
    }
    message("Done mapping files for source ", source_name, "...")
    # IF statement to only change source harm_mapped if all files from source are marked as initialized.
    needMap = query_mmdb(query_need_map_update, con_type = con_type)      
    if (!needMap$needmapcount) {
      send_statement_mmdb(query_need_map, con_type = con_type)
    }
  } else {
    message("All files mapped for source: ", source_name, "...") 
  }
}

#'@description A function to batch push harmonized data to harmonized tables.
#'@param df The data to batch.
#'@param f The file ID the data corresponds to.
#'@param type The data source data type (raw or aggregate).
#'@param map The harmonized variable map for a data source.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import dplyr
#'@return None. SQL statements update the MMDB database.
batch_map_source_file <- function(df = NULL, f = NULL, type = NULL, map = NULL, con_type = NULL){
  if(is.null(df)) return(message("Must provide data to push."))
  if(is.null(f)) return(message("Must provide the file ID."))
  if(is.null(type)) return(message("Must provide the data source data type: raw or aggregate."))
  if(is.null(map)) return(message("Must provide data source harmonized variable map."))
  
  # Create a temp table like the harmonized table to populate.
  send_statement_mmdb("DROP TABLE IF EXISTS temp_table2", con_type = con_type)
  if(con_type == "postgres"){
    query_temp = paste0("CREATE TABLE temp_table2 AS SELECT * FROM harmonized_", type," mytable WHERE 0")
    query_end_pos = paste0("SELECT file_id, record_id FROM ", paste0("harmonized_", type),
                           " WHERE file_id = ", f)
    query_update_file_mapped = paste0("UPDATE files SET mapped = TRUE WHERE file_id = '", f, "'")
  } else {
    query_temp = paste0("CREATE TABLE temp_table2 AS SELECT * FROM harmonized_", type," mytable WHERE 0")
    query_end_pos = paste0("SELECT files_fileid, record_id FROM ", paste0("harmonized_", type),
                           " WHERE files_fileid = ", f)
    query_update_file_mapped = paste0("UPDATE files SET mapped = 1 WHERE files.fileid = '", f, "'")
  }
  
  # Push mapped variable back to database.
  message("...Writing mapped variables to temp_table2 at: ", Sys.time()) 
  write_table_mmdb(name = 'temp_table2', data = df, con_type = con_type)
  # Push mapped variable back to database.
  message("...Updating harmonized table values at: ", Sys.time()) 
  
  # Update harmonized data table
  harmTbl <- paste0("harmonized_", type)
  tblID <- "record_id"
  tblFields <- query_mmdb("SELECT * FROM temp_table2", con_type = con_type) %>% names()
  vars <- map$to[map$source == source][(map$to[map$source == source] %in% tblFields)]
  
  # Generate a list of variables to update.
  varSet <- lapply(vars, function(x){ paste0(x, " = temp_table2.", x) }) %>% paste0(collapse = ", ")
  message("...Inserting new data at: ", Sys.time())
  if(con_type == "postgres"){
    query_batch = paste0("UPDATE ", harmTbl,
                         " SET ", varSet,
                         " FROM temp_table2",
                         " WHERE ", harmTbl, ".file_id = temp_table2.file_id AND ", harmTbl, ".record_id = temp_table2.record_id")
  } else {
    query_batch = paste0("UPDATE ", harmTbl,
                         " SET ", varSet,
                         " WHERE files_fileid = ", f)
  }
  send_statement_mmdb(query_batch, con_type = con_type)
  send_statement_mmdb("DROP TABLE IF EXISTS temp_table2", con_type = con_type)
  
  message("Finished mapping file data at: ", Sys.time())
  send_statement_mmdb(query_update_file_mapped, con_type = con_type)
}

######################################################################
# Run parameters
######################################################################

# Designate connection type.
con_type = "postgres"

# Designate the file path to the variable mapping file.
map_name <- "L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/map_all_variables_2024-07-26.csv"

# Read in the variable mapping file.
map <- read.csv(map_name, stringsAsFactors = F)

# Set to lowercase.
names(map) <- tolower(names(map))

# Filter to mapped fields. Standardize the "from" column for raw fields.
map <- filter(map, to != "") %>%
  mutate_at(vars("from"), list(~gsub('[[:punct:] ]+', ' ', trimws(tolower(.)))))

# Check for already processed sources.
if(con_type == "postgres"){
  query = paste0("SELECT DISTINCT source_name_abbr ",
                 "FROM source WHERE processed = FALSE AND loaded = TRUE")
} else {
  query = paste0("SELECT DISTINCT name ",
                 "FROM source WHERE processed = 0 AND loaded = 1")
}

# Generate a list of unprocessed sources.
unprocessed_sources <- query_mmdb(query, con_type = con_type) %>%
  unlist() %>% 
  unname()

# Read in the source data information control file & filter to source names that are unprocessed.
sources_process <- read.csv("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/monitoring_database_sources_2024-07-24.csv", colClasses = "character") %>%
  filter(process == 1, name %in% unprocessed_sources) %>%
  select(name) %>% unlist() %>% 
  unname()

# Process new sources.
if(!length(sources_process)){ 
  message("No new sources to process... Done.")
} else {
  # Begin loop through each data source.
  for (i in seq_len(length(sources_process))) { 
    source <- sources_process[i]
    if(con_type == "postgres"){
      query = paste0("SELECT source_id FROM source WHERE source_name_abbr = '", source, "'")
    } else {
      query = paste0("SELECT sourceid FROM source WHERE name = '", source, "'")
    }
    
    # Pull source ID(s) for sources to be processed.
    fk_source_id = query_mmdb(query, con_type = con_type) 
    
    message("Processing source ", source, ": ", i, " of ", length(sources_process), "...")
    # Generate mapping tables to create rows in the harmonized table to later be mapped.
    init_check = init_source_harmonized(source_id = fk_source_id, source_name = source, con_type = con_type)
    # Initialization didn't finish or pass for the source, skip mapping.
    if(!init_check){ next } 
    map_source_harmonized(source_id = fk_source_id, source_map = map[map$source == source, ], con_type = con_type)
    
    if(source == "epa_amtic") {
      dbSendStatement("UPDATE harmonized_raw SET reported_units = 'µg/m3' WHERE source_name_abbr = 'epa_amtic'", con_type = con_type)
    }
    
    # Update source table to reflect processed status.
    # IF statement to only change source source.processed if all files from source are marked as initialized.
    if(con_type == "postgres"){
      query_source_processed_check = paste0("SELECT SUM(CASE WHEN harm_init = FALSE OR harm_mapped = FALSE THEN 1 ELSE 0 END) needprocessing 
                              FROM source WHERE source_id = '", fk_source_id, "'")
      query_update_source_processed = paste0("UPDATE source SET processed = TRUE WHERE source_id = '", fk_source_id, "'")
    } else {
      query_source_processed_check = paste0("SELECT SUM(CASE WHEN harm_init = 0 OR harm_mapped = 0 THEN 1 ELSE 0 END) needprocessing 
                              FROM source WHERE sourceid = '", fk_source_id, "'")
      query_update_source_processed = paste0("UPDATE source SET processed = 1 WHERE sourceid = '", fk_source_id, "'")
    }
    needProcessing = query_mmdb(query_source_processed_check, con_type = con_type)
    if (!needProcessing$needprocessing) { 
      send_statement_mmdb(query_update_source_processed, con_type = con_type)
    }
  } # End loop over sources
  
  message("Done processing sources at: ", Sys.time())
}