## Script to load data from a data source into the source, files, documents, and raw data tables.
## Authors: Jonathan Taylor Wall, Kristin Isaacs, & Rachel Hencher
## Created: 2021-05-27
## Last updated: 2024-07-24

message("Running script to upload new source(s) at: ", Sys.time())
required.packages = c('DBI', 'plyr', 'dplyr', 'reshape2', 'magrittr')
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
      dbWriteTable(con, name =c('mmdb', name), value = data, row.names = F, overwrite = T)
    } else {
      dbWriteTable(con, name = name, value = data, row.names = F, overwrite = T)
    }
    
  },
  error = function(cond){ message("Error message: ", cond); return(NA) },
  warning = function(cond){ message("Warning message: ", cond); return(NULL) },
  finally = { dbDisconnect(con) })
}

#'@description A function to filter an input control file to data sources that have not been loaded 
#'into the MMDB database. Input file must have 'name' and 'load' fields.
#'@param file The file path to the input control file.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import dplyr
#'@return The input control file, filtered to data sources that need to be loaded to MMDB.
filter_to_unloaded <- function(file = NULL, con_type = NULL){
  if(is.null(file)) return(message("Must provide a filepath to load the control file."))
  
  # Get loaded sources.
  if(con_type == "postgres"){
    loaded_sources = query_mmdb(paste0("SELECT DISTINCT source_name_abbr FROM source ",
                                       "WHERE loaded = TRUE"),
                                con_type = con_type)
    # Filter out data sources already loaded.
    read.csv(file, colClasses = "character") %>% 
      filter(load == 1) %>%
      filter(!name %in% loaded_sources$source_name_abbr %>% unlist()) %>%
      return()
  } else {
    loaded_sources = query_mmdb(paste0("SELECT DISTINCT name FROM source ",
                                       "WHERE loaded = 1"),
                                con_type = con_type)
    # Filter out data sources already loaded.
    read.csv(file, colClasses = "character") %>% 
      filter(load == 1) %>%
      filter(!name %in% loaded_sources$name %>% unlist()) %>%
      return()
  }
}

#'@description A function to push a data source's information to the 'source' table in the MMDB database.
#'@param source Data source information for the data source of interest from the input control file.
#'@param root The root of the file path directory for the desired file.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@return The unique source ID value generated by the source table for data source.
push_source_table <- function(root = NULL, source = NULL, con_type = NULL){
  if(is.null(root)) return(message("Must provide a root directory path for data source files."))
  if(is.null(source)) return(message("Must provide the data source's abbreviated name."))
  
  message("Loading ", source$name, "...")
  
  # Get source ID by name.
  if(con_type == "postgres"){
    fk_source_id = query_mmdb(paste0("SELECT source_id FROM source WHERE source_name_abbr = '", source$name,"'"),
                              con_type = con_type) 
  } else {
    fk_source_id = query_mmdb(paste0("SELECT sourceid FROM source WHERE name = '", source$name,"'"),
                              con_type = con_type)  
  }
  
  # Check if this is a brand new source, or is already listed in the source table, with new files to load.
  if(nrow(fk_source_id) == 1){
    message("...Source already pushed to source table... Returning source ID."); return(fk_source_id)
  } else {# Completely new source to add = insert the source into the sources table.
    message("...Adding new source to source table.")
    vals <- paste(source$name, source$who, source$type, source$ICF_phase,
                source$description, source$full_source_name, sep = "','")
    if(con_type == "postgres"){
      send_statement_mmdb(paste0("INSERT INTO source (source_name_abbr, created_by, data_collection_type, OPPT_phase, description, full_source_name)",
                                 " VALUES ('", vals,"')"),
                          con_type = con_type) 
    } else {
      send_statement_mmdb(paste0("INSERT INTO source (name, who, type, OPPT_phase, description, full_source_name)",
                                 " VALUES ('", vals,"')"),
                          con_type = con_type)
    }
  }
  
  if(con_type =="postgres"){
    # Grab the ID of the source just inserted to add to the sample table.
    fk_source_id = query_mmdb(paste0("SELECT source_id FROM source WHERE source_name_abbr = '", source$name, "'"),
                              con_type = con_type)
  } else {
    # Grab the ID of the source just inserted to add to the sample table.
    fk_source_id = query_mmdb(paste0("SELECT sourceid FROM source WHERE name = '", source$name, "'"),
                              con_type = con_type)  
  }
  
  # Error handling for duplicates or unknown loading error
  if(nrow(fk_source_id) != 1){
    message("...Source ", source$name, " does not have a source_id or has duplicates... Cannot load source... Skipping...")
    return(NULL)
  } 
  return(fk_source_id)
}

#'@description A function to push a data source's file metadata to the files table in MMDB.
#'@param file The name of the file to push to the database.
#'@param loaded A list of files already loaded to the database.
#'@param source_id A data source's unique ID in MMDB.
#'@param source_info Information on the data source from the input control file.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@return None. SQL statements make updates to the database.
push_file_to_files <- function(file = NULL, loaded = NULL, source_id = NULL, source_info = NULL, clowder_id_list = NULL, con_type = NULL){
  if(is.null(file)) return(message("Must provide a filename to push."))
  if(is.null(loaded)) return(message("Must provide a list of loaded file names."))
  if(is.null(source_id)) return(message("Must provide the data source's unique ID."))
  if(is.null(source_info)) return(message("Must provide data source information from the control file."))
  
  # Check if file already loaded.
  if(!file %in% loaded$filename){
    message("Loading new file into file table.")
    # Get values to push to files table.
    vals <- paste(file, source_info$location[i], sep = "','")
    
    # Push to files table.
    if(con_type == "postgres"){
      send_statement_mmdb(paste0("INSERT INTO files ",
                                 "(filename, location, source_id) ",
                                 "VALUES ('", vals, "',", source_id, ")"),
                          con_type = con_type)  
    } else {
      # Push to files table
      send_statement_mmdb(paste0("INSERT INTO files ",
                                 "(filename, location, ",
                                 "source_sourceid) ",
                                 "VALUES ('", vals, "',", source_id, ")"),
                          con_type = con_type)        
    }
    return(message("...File loaded to files table."))
  } else { 
    message("...File already loaded into file table... Checking if data pushed.")
  }
}

#'@description A function to push a data source's file data to the corresponding raw table in MMDB. 
#'@param file The name of the file to push to the database
#'@param loaded A list of files already loaded to the database
#'@param type Whether the data source is a single-sample 'raw' or 'aggregate' data type
#'@param source_id A data source's unique ID in MMDB
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version
#'@return None. SQL statements make updates to the database.
push_file_to_raw <- function(file = NULL, loaded = NULL, type = NULL, source_id = NULL, con_type = NULL){
  if(is.null(file)) return(message("Must provide a filename to push."))
  if(is.null(loaded)) return(message("Must provide a list of loaded filenames."))
  if(is.null(type)) return(message("Must provide the data source type (raw or aggregate)."))
  if(is.null(source_id)) return(message("Must provide the data source's unique ID."))
  
  # Check if there are loaded files to filter from.
  if(nrow(loaded)){
    if(con_type == "postgres"){
      raw_init = query_mmdb(paste0("SELECT raw_init FROM files WHERE file_id = '", 
                                   loaded$file_id[loaded$filename == file], "'"),
                            con_type = con_type)
    } else {
      raw_init = query_mmdb(paste0("SELECT raw_init FROM files WHERE fileid = '", 
                                   loaded$fileid[loaded$filename == file], "'"),
                            con_type = con_type)  
    }
    
    # If file has raw_init value of 1 or TRUE, it has been loaded to raw table.
    if(raw_init$raw_init){ 
      return(message("...File data has been loaded to its raw data table... Moving on to next file..."))
    } else { message("...File data has not been loaded to its raw data table... Pushing data...") }
  } else {
    message("...File data has not been loaded to its raw data table... Pushing data...")
  }
  
  # Get the current file ID
  if(con_type == "postgres"){
    file_info = query_mmdb(paste0("SELECT * FROM files WHERE filename = '", file, "'"),
                           con_type = con_type)
  } else {
    file_info = query_mmdb(paste0("SELECT * FROM files WHERE filename = '", file, "'"),
                           con_type = con_type)
  }
  
  if(nrow(file_info) > 1) stop("Error: Duplicate file ID values matched...")
  
  # Load, prep, and push file data into raw table.
  load_path = paste0(sourcehome, sources_load$location)
  setwd(load_path); load_file = file
  message("...Loading csv: ", file)
  
  # Open up the data file csv (utf-8 encoding is important).
  datatab = read.csv(load_file, encoding = 'UTF-8', colClasses = "character")
  message("...File loaded... Formatting data")
  
  # Insert the number of records into the file table.
  if(con_type == "postgres"){
    send_statement_mmdb(paste0("UPDATE files SET n_records = '", nrow(datatab), "' WHERE file_id = '", file_info$file_id, "'"),
                        con_type = con_type)
  } else {
    send_statement_mmdb(paste0("UPDATE files SET n_records = '", nrow(datatab), "' WHERE fileid = '", file_info$file_id, "'"),
                        con_type = con_type)  
  }
  
  # Set record ID.
  datatab$record_id <- seq.int(nrow(datatab))
  
  # Convert to long form.
  melted_data <- melt(datatab, id.vars = c("record_id"), variable_name = "variable_name")
  # Set file ID.
  melted_data$files_fileid <- file_info$file_id
  # Set source ID.
  melted_data$files_source_sourceid <- source_id %>% unlist()
  colnames(melted_data)[which(colnames(melted_data) == "value")] <- "variable_value"  
  message("...Data formatted... Writing to temp table...")
  
  write_table_mmdb(name = "temp_table", data = melted_data, con_type = con_type)
  message("...Temp table written... Writing to raw data table...")
  
  if(con_type =="postgres"){
    send_statement_mmdb(paste0("INSERT INTO raw_", ifelse(type == "raw", "", "aggregate_"), "data",
                               " (record_id, variable_name, variable_value, file_id, ",
                               "source_id) select * from mmdb.temp_table"),
                        con_type = con_type)
    send_statement_mmdb(paste0("UPDATE files SET raw_init = TRUE WHERE file_id = '", file_info$file_id, "'"),
                        con_type = con_type)
    send_statement_mmdb("DROP table mmdb.temp_table", con_type = con_type)
  } else {
    send_statement_mmdb(paste0("INSERT INTO raw_", ifelse(type == "raw", "", "aggregate_"), "data",
                               " (record_id, variable_name, variable_value, files_fileid, ",
                               "files_source_sourceid) select * from temp_table"),
                        con_type = con_type)
    send_statement_mmdb(paste0("UPDATE files SET raw_init = 1 WHERE fileid = '", file_info$file_id, "'"),
                        con_type = con_type)  
    send_statement_mmdb("DROP table temp_table", con_type = con_type)
  }
  message("...File loaded to raw data successfully... Moving to next file...")
}

#'@description A function to push a data source's file metadata to the files table in MMDB.
#'@param source_id A data source's unique ID in MMDB.
#'@param docList A named list of file paths for documents and supplemental materials.
#'@param root The root of the file path directory for the desired file.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@return None. SQL statements make updates to the database.
push_doc_to_documents <- function(source_id = NULL, docList = NULL, log = NULL, root = NULL, con_type = NULL){
  if(is.null(source_id)) return(message("Must provide the data source's unique ID."))
  if(is.null(docList)) return(message("Must provide a named list of doucment/supplemental file names."))
  if(is.null(root)) return(message("Must provide the root directory for document files."))
  
  message("Inserting document data...")
  
  # Loop through data docs vs. supplemental doc types.
  for(doc in names(docList)){
    if(!is.na(docList[[doc]])){
      if(con_type == "postgres"){
        doc_init = query_mmdb(paste0("SELECT doc_init FROM documents WHERE source_id = '", source_id, "'"),
                              con_type = con_type)
      } else {
        doc_init = query_mmdb(paste0("SELECT doc_init FROM documents WHERE source_sourceid = '", source_id, "'"),
                              con_type = con_type)  
      }
      
      # Check if documents already loaded.
      if(!any(doc_init$doc_init)){
        #stop("Need to refine logic of what it means to add a record to the document's table. Reflect what files upload does...")
        
        # Insert file names and locations in documents table.
        filelist <- trimws(unlist(strsplit(docList[[doc]], ";")))
        for (j in seq_len(length(filelist))) {
          if(con_type == "postgres"){
            vals <- paste(doc, filelist[j], sep = "','")
            send_statement_mmdb(paste0("INSERT INTO documents ",
                                       "(type, filename, ",
                                       "source_id) ",
                                       "VALUES ('",
                                       vals, "',", source_id, ")"),
                                con_type = con_type)
          } else {
            vals <- paste(doc, filelist[j], sep = "','")
            send_statement_mmdb(paste0("INSERT INTO documents ",
                                       "(type, filename, ",
                                       "source_sourceid) ",
                                       "VALUES ('",
                                       vals, "',", source_id, ")"),
                                con_type = con_type) 
          }
          fk_file_id = query_mmdb(paste0("SELECT documentid FROM documents WHERE filename = '", filelist[j], "'"),
                                  con_type = con_type)
          send_statement_mmdb(paste0("UPDATE documents SET doc_init = TRUE WHERE documentid = '", fk_file_id[1,1], "'"),
                              con_type = con_type)
        }
      } else {
        message("...", doc, " documents already loaded... Moving on.")
      }
    } else { 
      message("...No ", doc, " documents to load... Moving on.")
    }
  }
}

#'@description A function to check if all data source files, documents, and supplements are loaded 
#'into the database. This signifies the data source has successfully been initialized into the database.
#'@param source_id The unique database source ID value from the source table for the data source to check.
#'@param source_name The abbreviated name for the data source (e.g. ahhs, ctd).
#'@param source_info Data source information for the data source of interest from the input control file.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@import DBI dplyr
#'@return Boolean of whether all files, documents, and supplemental material has been loaded into MMDB 
#'files and documents tables.
check_files_loaded <- function(source_id = NULL, source_name = NULL, source_info = NULL, con_type = NULL){
  if(is.null(source_id)) return(message("Must provide the data source's unique ID"))
  if(is.null(source_name)) return(message("Must provide the data source's abbreviated name"))
  if(is.null(source_info)) return(message("Must provide the data source's control file information"))
  
  if(con_type == "postgres"){
    query1 = paste0("SELECT file_id, location, filename ",
                    "FROM files WHERE source_id = ", 
                    source_id, " AND raw_init = TRUE")
    query2 = paste0("SELECT documentid, filename ",
                    "FROM documents where source_id = ",
                    source_id, " AND doc_init = TRUE")    
  } else {
    query1 = paste0("SELECT fileid, location, filename ",
                    "FROM files WHERE source_sourceid = ", 
                    source_id, " AND raw_init = 1")
    query2 = paste0("SELECT documentid, filename ",
                    "FROM documents where source_sourceid = ",
                    source_id, " AND doc_init = 1")
  }
  
  # Get list of loaded files.
  loaded_files = query_mmdb(query1, con_type = con_type)
  # Get list of loaded documents.
  loaded_docs = query_mmdb(query2, con_type = con_type)
  # Get expected file directory/location.
  fileDir = source_info %>%
    filter(name == source_name) %>%
    select(location) %>% unlist()
  # Get expected file names.
  fileList = source_info %>%
    filter(name == source_name) %>%
    select(filename) %>% unlist() %>%
    strsplit(., ";") %>% as.data.frame(stringsAsFactors = FALSE) %>%
    mutate(filename = trimws(filename)) %>%
    mutate(location = fileDir)
  # Get expected document names.
  docList = source_info %>%
    filter(name == source_name) %>%
    select(filename = data_documents) %>% unlist() %>%
    strsplit(., ";") %>% as.data.frame(stringsAsFactors = FALSE) %>%
    mutate(filename = trimws(filename)) %>%
    filter(!is.na(filename))
  # Get expected supplemental file names.
  suppList = source_info %>%
    filter(name == source_name) %>%
    select(filename = supplemental_documents) %>% unlist() %>%
    strsplit(., ";") %>% as.data.frame(stringsAsFactors = FALSE) %>%
    mutate(filename = trimws(filename)) %>%
    filter(!is.na(filename)) 
  tmp = left_join(fileList, loaded_files, by = c("location", "filename"))
  
  # Join dataframes to match to file ID or doc ID (if loaded).
  if(nrow(docList)){
    tmp2 = left_join(docList, loaded_docs, by = "filename")  
  } else {
    tmp2 = data.frame(documentid = 1)
  }
  if(nrow(suppList)){
    tmp3 = left_join(suppList, loaded_docs, by = "filename")  
  } else {
    tmp3 = data.frame(documentid = 1)
  }
  # File, document, or supplemental material NOT loaded if doesn't match to a file ID/doc ID (NA value).
  return(any(is.na(tmp$file_id)) | any(is.na(tmp2$documentid)) | any(is.na(tmp3$documentid)))
}

#'@description A control flow function to orchestrate all helper functions to push a data source to MMDB.
#'@param sourcehome The root directory for the data source files.
#'@param sources_load A filtered dataframe of the input control file of data sources to load.
#'@param con_type Whether to connect to PostgreSQL, MySQL, or SQLite version.
#'@return None. SQL statements make updates to the database.
load_source_to_mmdb <- function(sourcehome = NULL, sources_load = NULL, con_type = NULL){
  if(is.null(sourcehome)) return(message("Must provide the root directory for data source files."))
  if(is.null(sources_load)) return(message("Must provide data source information from the control file."))

  fk_source_id = push_source_table(root = sourcehome, source = sources_load, con_type = con_type)
  if(is.null(fk_source_id)) next
  # Load the file and data document information for this data source, then parse the data file names.
  # Pull list of files already loaded to the files table, but perhaps not loaded into raw_data or raw_aggregate_data tables.
  filelist <- trimws(unlist(strsplit(sources_load$filename, ";")))
  if(con_type == "postgres"){
    loadedFiles <- query_mmdb(paste0("SELECT * FROM files WHERE source_id = '", fk_source_id, "'"),
                              con_type = con_type)
  } else {
    loadedFiles <- query_mmdb(paste0("SELECT * FROM files WHERE source_sourceid = '", fk_source_id, "'"),
                              con_type = con_type)  
  }
  
  if(length(filelist)){
    # Loop through each file.
    for (j in seq_len(length(filelist))){
      message("Loading file ", j, " of ", length(filelist), " : ", filelist[j])
      
      # Insert file names and locations into files table.
      push_file_to_files(file = filelist[j], loaded = loadedFiles, source_id = fk_source_id, 
                         source_info = sources_load, con_type = con_type)
      # Insert file data into raw table.
      push_file_to_raw(file = filelist[j], loaded = loadedFiles, type = sources_load$type, source_id = fk_source_id, 
                       con_type = con_type)
    }
  } else { message("...No file names to load... Moving on.") }
  
  # Get documents to load.
  docs <- list(data = sources_load$data_documents, 
               supplemental = sources_load$supplemental_documents)
  # Load document info to documents table.
  push_doc_to_documents(source_id = fk_source_id, docList = docs,  
                        root = paste0(sourcehome, sources_load$location), con_type = con_type)
  
  message("Finished loading source ", sources_load$name)
  
  # Flag data source loading as complete if all files, documents, and supplements are loaded.
  if(!check_files_loaded(source_id = fk_source_id$source_id, source_name = sources_load$name, source_info = sources_load,
                         con_type = con_type)){
    if(con_type == "postgres"){
      send_statement_mmdb(paste0("UPDATE source SET loaded = TRUE WHERE source_id = '", fk_source_id, "'"),
                          con_type = con_type)
    } else {
      send_statement_mmdb(paste0("UPDATE source SET loaded = 1 WHERE source.sourceid = '", fk_source_id, "'"),
                          con_type = con_type)
    }
  } else {
    message("...Not all files loaded for: ", sources_load$name)
  }
  message("...Done at: ", Sys.time())
}

######################################################################
# Run parameters
######################################################################

# Designate path to directory for raw data source files (MUST end with forward slash!).
sourcehome <- "L:/Lab/CCTE_MMDB/MMDB/MMDB_Data/"

# Designate connection type.
con_type = "postgres"

# Read in the source data information from control file.
sources_load <- filter_to_unloaded(file = "L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/monitoring_database_sources_2024-07-24.csv", con_type = con_type)

if(!nrow(sources_load)) {
  message("No new sources to load... Done at: ", Sys.time()) 
} else {
  # Load unloaded sources to MMDB
  for (i in seq_len(nrow(sources_load))) {
    load_source_to_mmdb(sourcehome = sourcehome, sources_load = sources_load[i, ], 
                        con_type = con_type)  
  }
}