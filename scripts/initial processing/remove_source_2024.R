## Script to remove MMDB records from ALL tables for a data source by its associated source table source_id.
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: 2021-05-28
## Last updated: 2024-07-24

print("Running script to remove indicated source(s)...")
required.packages = c('DBI', 'dplyr', 'reshape2')
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

######################################################################
# Run Parameters
######################################################################

# Designate path to source file.
args <- "L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/monitoring_database_sources_2024-07-24.csv"
# If running the script from the command line, an alternate statement for 'args' can found directly below (uncomment to use).
#args <- commandArgs(trailingOnly = TRUE)

# Designate connection type.
con_type = "postgres"

# Create empty vectors to later hold source info & data type(s).
sourceList <- c()
type <- c()

# Load data from source csv if args is a file path that exists.
if (file.exists(args)) {
  print("Pulling sources to remove.")
  dat = read.csv(args, stringsAsFactors = FALSE)
  sourceList = dat$name[dat$delete == 1 | dat$load == 0]
  type = dat$type[dat$delete == 1 | dat$load == 0]
} else { 
  stop("Error: Cannot find control file from supplied file path. Check working directory and/or file name.")
}

# Print list of sources to be removed and corresponding data types.
if (!length(sourceList)) {
  print("No sources were passed to be removed... Done.")
} else {
  print(paste0("Sources: ", paste(sourceList, collapse = ", "), "Types: ", paste(type, collapse = ", ")))
  if (any(is.na(type)) | (length(sourceList) != length(type))) { 
    stop("Missing crucial 'type' input... Stopping...") 
  }
  
  # Loop through each data source identified for deletion.
  for (i in 1:length(sourceList)) {
    print(paste0("Deleting source ", i, " of ", length(sourceList), " ", sourceList[i], "..."))
    # Check valid source type.
    if (type[i] %in% c("aggregate", "raw")) {
      # Pull source ID & file ID lists.
      if(con_type == "postgres"){
        fk_source_id = query_mmdb(paste0("SELECT source_id as sourceid FROM source where source_name_abbr = '", 
                                         sourceList[i], "'"), con_type = con_type)
        fk_file_id = query_mmdb(paste0("SELECT file_id as fileid FROM files WHERE source_id = '", fk_source_id, "'"), con_type = con_type)
      } else {
        fk_source_id = query_mmdb(paste0("SELECT sourceid FROM source where name = ", sourceList[i]), con_type = con_type)  
        fk_file_id = query_mmdb(paste0("SELECT fileid FROM files WHERE source_sourceid = '", fk_source_id, "'"), con_type = con_type)
      }
      # Check for valid source ID.
      if (nrow(fk_source_id) != 1) {
        print(paste0("Source ", sourceList[i], " does not have a source ID or has duplicates... Cannot remove processing. Skipping..."))
        next
      }
      # Create list of database tables to delete from with associated key fields.
      if(con_type == "postgres"){
        # List of tables & associated keys to remove by.
        dbtables <- list(tbl = c(paste0("raw_", ifelse(type[i] == "raw", "", "aggregate_"), "data"),
                                 "files", "documents", "source"),
                         keys = list(list("source_id ='", "file_id IN ("), 
                                     "source_id ='", "source_id ='", "source_id ='"))
      } else {
        # List of tables & associated keys to remove by.
        dbtables <- list(tbl = c(paste0("raw_", ifelse(type[i] == "raw", "", "aggregate_"), "data"),
                                 "files", "documents", "source"),
                         keys = list(list("files_source_sourceid = '", "files_fileid IN ("), 
                                     "source_sourceid = '", "source_sourceid = '", "sourceid = '"))  
      }
      
      # Loop through each database table in list.
      for (j in seq_len(length(dbtables$tbl))) {
        # Remove sources from associated database tables.
        if(sourceList[i] == "usgs"){
          stop("Need to update logic for USGS for PostgreSQL...")
          # For now just use this process to remove the large USGS data set.
          # For SQL purposes, it is faster to copy the table without the associated rows, truncate, then copy back over...
          message("Creating temp_copy table of ", dbtables$tbl[j], " at: ", Sys.time())
          dbSendStatement(tesladb, paste0("CREATE TABLE temp_copy LIKE ", dbtables$tbl[j]))
          message("Populating temp_copy table with ", dbtables$tbl[j]," table (except selected source: ", sourceList[i]," ) at: ", Sys.time())
          dbSendStatement(tesladb,paste0("INSERT INTO temp_copy SELECT * FROM ", dbtables$tbl[j]," WHERE files_source_sourceid <> ", fk_source_id))
          message("Truncating ", dbtables$tbl[j], " table at: ", Sys.time())
          # Clear out raw_data table.
          dbSendStatement(tesladb, paste0("TRUNCATE TABLE ", dbtables$tbl[j])) 
          message("Re-populating ", dbtables$tbl[j], " table with temp_copy data at: ", Sys.time())
          dbSendStatement(tesladb,paste0("INSERT INTO ", dbtables$tbl[j], " SELECT * FROM temp_copy"))
          # Drop unneeded temp_copy table.
          message("Dropping temp_copy table at: ", Sys.time())
          dbSendStatement(tesladb, "DROP TABLE temp_copy") 
        } else {
          print(paste0("DELETING ",  sourceList[i] ," FROM ", dbtables$tbl[j]))
          query = paste0("DELETE FROM ", dbtables$tbl[j], " WHERE ", dbtables$keys[[j]][1], fk_source_id, "'", 
                         ifelse(is.na(dbtables$keys[[j]][2]), "", paste0(" AND ", dbtables$keys[[j]][2], paste0(fk_file_id$fileid, collapse = ", "), ")")))
          print(paste0("Query: ", query))
          send_statement_mmdb(query, con_type)
        }
      }
    }
    # Clear results set.
    if (length(dbListResults(conn))) { print("Clearing query result sets."); dbClearResult(dbListResults(conn)[[1]]) }
  }
  print("Done.")
}