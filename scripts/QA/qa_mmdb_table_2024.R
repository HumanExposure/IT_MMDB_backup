## Script to review all tables for any discrepancies based on expected counts
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: Unknown
## Last updated: 2024-08-14

message("Running script to QA MMDB tables...")
required.packages = c('DBI', 'dplyr', 'magrittr', 'purrr')
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

#'@description A helper function to query MMDB and receive the results. 
#'Handles errors/warnings with tryCatch.
#'@param query A SQL query string to query the database with.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import DBI dplyr magrittr
#'@return Dataframe of database query results.
query_mmdb <- function(query = NULL, con_type){
  if(is.null(query)) return(message("Must provide a query to send"))
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

######################################################################
# Run Parameters
######################################################################

# Set directory to raw datasource file directory
source_home <- "L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/"
setwd(source_home)
con_type = "postgres"

# Read in the source data information
control_file <- read.csv("input/monitoring_database_sources_2024-07-24.csv", colClasses = "character") %>% filter(load == 1)
# Standardize the "from" column for raw fields
map <- read.csv("input/map_all_variables_2024-07-26.csv", stringsAsFactors = F)
names(map) <- tolower(names(map))
map <- filter(map, to != "") %>%
  mutate_at(vars("from"), list(~gsub('[[:punct:] ]+',' ', trimws(tolower(.)))))

if(con_type == "postgres"){
  src = query_mmdb("SELECT DISTINCT source_id as sourceid, source_name_abbr as name, data_collection_type as type FROM source", con_type = con_type)
  files = query_mmdb("SELECT DISTINCT source_id as source_sourceid, file_id as fileid FROM files", con_type = con_type) 
} else {
  src = query_mmdb("SELECT DISTINCT sourceid, name, type FROM source", con_type = con_type)
  files = query_mmdb("SELECT DISTINCT source_sourceid, fileid FROM files", con_type = con_type)
}

# Check if any names found in database that were not supposed to be loaded
tmp = src$name[!(src$name %in% control_file$name)]
message("Extraneous names in source table: ", ifelse(is_empty(tmp), 0, tmp))

tables <- c("raw_data", "raw_aggregate_data", "harmonized_raw", "harmonized_aggregate")
extra_source <- sapply(tables, function(x){
  if(con_type == "postgres"){
    tmp = query_mmdb(paste0("SELECT DISTINCT source_id FROM ", x,
                            " WHERE source_id NOT IN (", toString(src$sourceid), ")"),
                     con_type = con_type)
  } else {
    tmp = query_mmdb(paste0("SELECT DISTINCT files_source_sourceid FROM ", x,
                            " WHERE files_source_sourceid NOT IN (", toString(src$sourceid), ")"),
                     con_type = con_type)
  }
  
  return(paste0("Extraneous source IDs in ", x, ": ", ifelse(is_empty(tmp), 0, tmp)) %T>%
           message()
  )
})

missing_source <- sapply(tables, function(x){
  if(x %in% c("raw_aggregate_data", "harmonized_aggregate")){
    sourceid <- src %>% filter(type == "aggregate")
  } else {
    sourceid <- src %>% filter(type == "raw")
  }
  if(con_type == "postgres"){
    tmp = query_mmdb(paste0("SELECT DISTINCT source_id as files_source_sourceid FROM ", x),
                     con_type = con_type)
  } else {
    tmp = query_mmdb(paste0("SELECT DISTINCT files_source_sourceid FROM ", x),
                     con_type = con_type)
  }
  
  tmp = sourceid$sourceid[!(sourceid$sourceid %in% tmp$files_source_sourceid)]
  return(paste0("Missing source IDs from ", x, ": ", ifelse(is_empty(tmp), 0, tmp)) %T>%
           message()
  )
})

extra_files <- sapply(tables, function(x){
  if(con_type == "postgres"){
    tmp = query_mmdb(paste0("SELECT DISTINCT file_id FROM ", x,
                            " WHERE file_id NOT IN (", toString(files$fileid), ")"),
                     con_type = con_type)
  } else {
    tmp = query_mmdb(paste0("SELECT DISTINCT files_source_sourceid, files_fileid FROM ", x,
                            " WHERE files_fileid NOT IN (", toString(files$fileid), ")"),
                     con_type = con_type)
  }
  return(paste0("Extraneous file IDs in ", x, ": ", ifelse(is_empty(tmp), 0, tmp)) %T>%
           message()
  )
})

missing_files <- sapply(tables, function(x){
  if(x %in% c("raw_aggregate_data", "harmonized_aggregate")){
    fileid <- files %>% filter(source_sourceid %in% (src %>% filter(type == "aggregate") %>% select(sourceid) %>% unlist()))
  } else {
    fileid <- files %>% filter(source_sourceid %in% (src %>% filter(type == "raw") %>% select(sourceid) %>% unlist()))
  }
  
  if(con_type == "postgres"){
    tmp = query_mmdb(paste0("SELECT DISTINCT file_id as files_fileid FROM ", x),
                     con_type = con_type)
  } else {
    tmp = query_mmdb(paste0("SELECT DISTINCT files_source_sourceid, files_fileid FROM ", x),
                     con_type = con_type)
  }
  
  tmp = fileid$fileid[!(fileid$fileid %in% tmp$files_fileid)]
  
  return(paste0("Missing file IDs from ", x, ": ", ifelse(is_empty(tmp), 0, tmp)) %T>%
           message()
  )
})


# Check for extraneous files in files and document table
expected_files <- lapply(control_file$filename, 
                        function(x){ data.frame(files = trimws(unlist(strsplit(x, ";")))) 
                        }) %>% bind_rows()
files = query_mmdb("SELECT * FROM files", con_type = con_type) %>% mutate(across(filename, ~trimws(.)))

tmp = files$filename[!(files$filename %in% expected_files$files)]
message("Extraneous files in file table: ", ifelse(is_empty(tmp), 0, tmp))

tmp = expected_files$files[!(expected_files$files %in% files$filename)]
message("Missing files: ", ifelse(is_empty(tmp), 0, tmp))

expected_data_docs <- lapply(control_file$data_documents, 
                           function(x){ data.frame(datadocs = trimws(unlist(strsplit(x, ";")))) 
                           }) %>% bind_rows()
expected_supp_docs <- lapply(control_file$supplemental_documents, 
                           function(x){ data.frame(suppdocs = trimws(unlist(strsplit(x, ";")))) 
                           }) %>% bind_rows()
docs = query_mmdb("SELECT * FROM documents", con_type = con_type)
data_docs <- docs %>% filter(type == "data") %>% select(filename) %>% mutate(across(filename, ~trimws(.)))
data_docs$filename[data_docs$filename == "NA"] <- NA

tmp = data_docs$filename[!(data_docs$filename %in% expected_data_docs$datadocs)]
message("Extraneous data documents in docs table: ", ifelse(is_empty(tmp), 0, tmp))

tmp = expected_data_docs$datadocs[!(expected_data_docs$datadocs %in% data_docs$filename)]
message("Missing data documents: ", ifelse(is_empty(tmp), 0, tmp))

supp_docs <- docs %>% filter(type == "supplemental") %>% select(filename) %>% mutate(across(filename, ~trimws(.)))
supp_docs$filename[supp_docs$filename == "NA"] <- NA
tmp = supp_docs$filename[!(supp_docs$filename %in% expected_supp_docs$suppdocs)]
message("Extraneous supplemental documents in docs table: ", ifelse(is_empty(tmp), 0, tmp))

tmp = expected_supp_docs$suppdocs[!(expected_supp_docs$suppdocs %in% supp_docs$filename)]
message("Missing supplemental documents: ", ifelse(is_empty(tmp), 0, tmp))





#Check for record count matches
#Cannot check this anymore because we remove nonchemical records from harmonized tables...
# harmMismatch <- lapply(c("harmonized_aggregate", "harmonized_raw"), function(x){
#   aggRecords <- tbl(con, x) %>% 
#     select(files_source_sourceid, files_fileid, record_id) %>% 
#     group_by(files_fileid) %>% 
#     summarise(actualN=n()) %>% 
#     collect() %>%
#     left_join(tbl(con, "files") %>% select(fileid, n_records) %>% rename(expectedN = n_records) %>% collect(), by=c("files_fileid"="fileid")) %>%
#     mutate(match = eval(actualN == expectedN))
#   tmp = paste(aggRecords$files_fileid[aggRecords$match == F], collapse="; ")
#   message("Records Mismatch for ",x," files: ", ifelse(is_empty(tmp), 0, tmp))
# })
stop("Need to account for records removed from harmonized tables due to not being chemical records...")
rawMismatch <- lapply(c("raw_aggregate_data", "raw_data"), function(x){
  if(con_type == "postgres"){
    aggRecords = query_mmdb(paste0("SELECT DISTINCT source_id as files_source_sourceid, file_id as files_fileid, record_id FROM ", x), 
                            con_type = con_type) %>%
      group_by(files_fileid) %>% 
      summarise(actualN = n()) %>% 
      left_join(query_mmdb("SELECT file_id as fileid, n_records FROM files", 
                           con_type = con_type) %>%
                  dplyr::rename(expectedN = n_records), 
                by = c("files_fileid" = "fileid")) %>%
      mutate(match = eval(actualN == expectedN))
  } else {
    aggRecords = query_mmdb(paste0("SELECT DISTINCT files_source_sourceid, files_fileid, record_id FROM ", x), 
                            con_type = con_type) %>%
      group_by(files_fileid) %>% 
      summarise(actualN = n()) %>% 
      left_join(query_mmdb("SELECT fileid, n_records FROM files", 
                           con_type = con_type) %>%
                  dplyr::rename(expectedN = n_records), 
                by = c("files_fileid" = "fileid")) %>%
      mutate(match = eval(actualN == expectedN))
  }
  # aggRecords <- tbl(con, x) %>% 
  #   select(files_source_sourceid, files_fileid, record_id) %>% 
  #   distinct() %>%
  #   group_by(files_fileid) %>% 
  #   summarise(actualN=n()) %>% 
  #   collect() %>%
  #   left_join(tbl(con, "files") %>% select(fileid, n_records) %>% rename(expectedN = n_records) %>% collect(), by=c("files_fileid"="fileid")) %>%
  #   mutate(match = eval(actualN == expectedN))
  tmp = paste(aggRecords$files_fileid[aggRecords$match == F], collapse = "; ")
  message("Records mismatch for ", x, " files: ", ifelse(is_empty(tmp), 0, tmp))
})