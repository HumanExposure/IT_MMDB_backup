## Script to map/update substance and media table IDs to harmonized tables if substance/media tables are updated/changed.
## Authors: Kristin Isaacs, Jonathan Taylor Wall, & Rachel Hencher
## Created: Unknown
## Last Updated: 2024-08-07

message("Running script to update substance/media tables...")
required.packages = c('DBI', 'dplyr', 'magrittr', 'lubridate')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

############################################################
# Functions
############################################################

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
  if(is.null(name)) return(message("Must provide a name for the database table"))
  if(is.null(data)) return(message("Must provide data to push to the database table"))
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
  finally =  { dbDisconnect(con) })
}

############################################################
# Run Parameters
############################################################

# Designate connection type.
con_type = "postgres"

# Select which table to update, or both.
run_type = "s"

if(con_type == "postgres"){
  sub_list <- list(
    media = list(
      id = list("media_id", "idmedia"),
      data = {query_mmdb("SELECT source_id, reported_media, reported_species, media_id as idmedia FROM media", con_type = con_type) %>%
          mutate(across(c("reported_media", "reported_species"), ~trimws(tolower(.x)))) # Standardize for matching purposes
      },
      mergeBy = c("source_id", "reported_media", "reported_species")),
    substances = list(
      id = list("substance_id", "idsubstance"),
      data = {
        query_mmdb("SELECT source_id, reported_chemical_name, reported_casrn, substance_id as idsubstance FROM substances", con_type = con_type) %>% 
          mutate(reported_chemical_name = trimws(tolower(reported_chemical_name))) # Standardize for matching purposes
      },
      mergeBy = c("source_id", "reported_chemical_name", "reported_casrn"))
  )
  
  # Create master list of fields to use in the update queries by harmonized table
  tbls <- list(harmonized_raw = list(id = "harmonized_raw_id",
                                     substances = list(
                                       select = c("harmonized_raw_id", "reported_chemical_name", "reported_casrn", "source_id"), 
                                       mutate = "reported_chemical_name",
                                       merge = c("harmonized_raw_id", "idsubstance")
                                     ),
                                     media = list(
                                       select = c("harmonized_raw_id", "reported_media", "reported_species", "source_id"),
                                       mutate = c("reported_media", "reported_species"),
                                       merge = c("harmonized_raw_id", "idmedia")
                                     )
                                     ),
               harmonized_aggregate = list(id = "harmonized_aggregate_id",
                                           substances = list(
                                             select = c("harmonized_aggregate_id", "reported_chemical_name", "reported_casrn", "source_id"),
                                             mutate = "reported_chemical_name",
                                             merge = c("harmonized_aggregate_id", "idsubstance")
                                             ),
                                           media = list(
                                             select = c("harmonized_aggregate_id", "reported_media", "reported_species", "source_id"),
                                             mutate = c("reported_media","reported_species"),
                                             merge = c("harmonized_aggregate_id","idmedia")
                                             )
                                           )
               )
  } else {
    sub_list <- list(
      media = list(
        id = list("media_idmedia", "idmedia"),
        data = {tbl(con, "media") %>% 
            select(source_sourceid, reported_media, reported_species, idmedia) %>%
            collect() %>% # Select distinct ignores case, so we merge on lowercase (both datasets))
            mutate(across(c("reported_media", "reported_species"), ~trimws(tolower(.x)))) # Standardize for matching purposes
          },
        mergeBy = c("files_source_sourceid" = "source_sourceid", "reported_media", "reported_species")),
      substances = list(
        id = list("substances_idsubstance", "idsubstance"),
        data = {
          temp = tbl(con, "substances") %>% 
            select(source_sourceid, reported_chemical_name,reported_casrn,idsubstance) %>% 
            collect() %>% # Select distinct ignores case, so we merge on lowercase (both datasets)
            mutate(reported_chemical_name = trimws(tolower(reported_chemical_name))) # Standardize for matching purposes
          },
        mergeBy = c("reported_chemical_name", "reported_casrn"))
      )
    
    # Create master list of fields to use in the update queries by harmonized table
    tbls <- list(harmonized_raw = list(id = "idharmonized_raw",
                                       substances = list(
                                         select = c("idharmonized_raw", "reported_chemical_name", "reported_casrn", "files_source_sourceid"),
                                         mutate = "reported_chemical_name",
                                         merge = c("idharmonized_raw", "idsubstance")
                                         ),
                                       media = list(
                                         select = c("idharmonized_raw", "reported_media", "reported_species", "files_source_sourceid"),
                                         mutate = c("reported_media","reported_species"),
                                         merge = c("idharmonized_raw","idmedia")
                                         )
                                       ), 
                 harmonized_aggregate = list(id = "idharmonized_aggregate",
                                             substances = list(
                                               select = c("idharmonized_aggregate", "reported_chemical_name", "reported_casrn", "files_source_sourceid"),
                                               mutate = "reported_chemical_name",
                                               merge = c("idharmonized_aggregate", "idsubstance")
                                               ),
                                             media = list(
                                               select = c("idharmonized_aggregate", "reported_media", "reported_species", "files_source_sourceid"),
                                               mutate = c("reported_media", "reported_species"),
                                               merge = c("idharmonized_aggregate", "idmedia")
                                               )
                                             )
    )
    }

# Create ID table with source and file information
source_table = query_mmdb("SELECT * FROM source", con_type = con_type) %>% rename_all(function(x) paste0("src.", x))

if(con_type == "postgres"){
  query_id_table = paste0("SELECT src.source_id as sourceid, src.source_name_abbr as name, src.data_collection_type as type, f.file_id as fileid ",
                          "FROM source as src ",
                          "LEFT JOIN mmdb.files as f on src.source_id = f.source_id")
} else {
  query_id_table = paste0("SELECT src.sourceid, src.name, src.type, f.fileid ",
                          "FROM source as src ",
                          "LEFT JOIN files as f on src.sourceid = f.source_sourceid")
}
id_table = query_mmdb(query_id_table, con_type = con_type) %>%
  filter(!is.na(fileid))

# Control flow to select which table to update, or both
if(run_type == "s"){
  sub_list_names <- "substances"
} else if (run_type == "m"){
  sub_list_names <- "media"
} else {
  sub_list_names <- names(sub_list)
}
# Subset which source to update. Comment out if updating all sources.
id_table <- filter(id_table, sourceid == 150)

# Start of merging and reassignment of table IDs
for(subs in sub_list_names){ # Loop through media/substances table
  for(f in id_table$fileid){ # Loop through each data source file
    tbl <- paste0("harmonized_", id_table$type[id_table$fileid == f])
    src <- id_table$sourceid[id_table$fileid == f]
    message("Updating table....", tbl, " with ", subs, " ID... For source ", src, 
            " and file ", f, " (", which(id_table$fileid == f), " of ", length(id_table$fileid), ") at ", Sys.time())
    # Get list of fields to select or mutate
    selectList <- tbls[[tbl]][[subs]]$select
    mutateList <- tbls[[tbl]][[subs]]$mutate
    mergeList <- tbls[[tbl]][[subs]]$merge
    # Pull harmonized data, filtering and selecting by fields
    if(con_type == "postgres"){
      query_raw = paste0("SELECT ", toString(selectList), " FROM ", tbl, " WHERE ",
                         "source_id = ", src, " AND file_id = ", f)
    } else {
      query_raw = paste0("SELECT ", toString(c(tbls[[tbl]]$id, selectList)), " FROM ", tbl, " WHERE ",
                         "files_source_sourceid = ", src, " AND files_fileid = ", f)
    }
    
    rawvals <- query_mmdb(query_raw, con_type) %>% #tbl(con, tbl) %>% 
      mutate(across(all_of(mutateList), ~trimws(tolower(.x)))) # Standardize for matching purposes
    if(subs == "substances"){
      # Fix erroneous conversion to date format --> Rearrange to correct format
      if(length(rawvals$reported_casrn[grepl("/", rawvals$reported_casrn)])){
        rawvals$reported_casrn[grepl("/", rawvals$reported_casrn)] <- sapply(strsplit(rawvals$reported_casrn[grepl("/", rawvals$reported_casrn)], "/"), function(x){
          if(as.numeric(x[1]) < 10){ x[1] <- paste0(0, x[1]) }
          return(paste(x[3], x[1], x[2],sep = "-"))
        })
      }
      # Removed because we now harmonize missing NA during initial processing
      rawvals$reported_casrn[rawvals$reported_casrn %in% c("", " ", "NA", "na", "<NA>", "<na>", "NULL", "null")] <- NA # Replace empty values with NA
      rawvals$reported_chemical_name[rawvals$reported_chemical_name %in% c("", " ", "<NA>", "<na>", "NULL", "null")] <- NA # Replace empty values with NA  
      # Quick special character fix
      rawvals$reported_chemical_name[rawvals$reported_chemical_name == "benzo(j)fluoranthčne"] = "benzo(j)fluoranthene"
      rawvals$reported_chemical_name[rawvals$reported_chemical_name == "m-xylčne/p-xylčne (somme)"] = "m-xylene/p-xylene (somme)"
      rawvals$reported_chemical_name[rawvals$reported_chemical_name == "somme(b,j,k)-benzofluoranthčnes"] = "somme(b,j,k)-benzofluoranthenes"
      rawvals$reported_chemical_name[rawvals$reported_chemical_name == "xylčnes totaux"] = "xylenes totaux"
      
    } else if (subs == "media") {
      if(con_type == "postgres"){
        rawvals <- rawvals %>% 
          left_join(source_table %>% select(src.source_id, src.source_name_abbr), by = c("source_id" = "src.source_id"))
      } else {
        rawvals <- rawvals %>% 
          left_join(source_table %>% select(src.sourceid, src.name), by = c("files_source_sourceid" = "src.sourceid"))  
      }
      
      rawvals$reported_media[rawvals$reported_media %in% c("", " ")] <- NA # Replace empty values with NA
      rawvals$reported_species[rawvals$reported_species %in% c("", " ")] <- NA # Replace empty values with NA
    }
    message("Pulled data for ", tbl, ": ", Sys.time())
    # Join to match by substance/media fields leaving harmonized ID and substance/media ID values to push
    if(con_type == "postgres"){
      merged <- left_join(rawvals, 
                          sub_list[[subs]]$data %>% 
                            filter(source_id == id_table$sourceid[id_table$fileid == f]), # Filter to source ID
                          by = sub_list[[subs]]$mergeBy) %>% 
        select(all_of(mergeList))
    } else {
      merged <- left_join(rawvals, 
                          sub_list[[subs]]$data %>% 
                            filter(source_sourceid == id_table$sourceid[id_table$fileid == f]), # Filter to source ID
                          by = sub_list[[subs]]$mergeBy) %>% 
        select(all_of(mergeList))  
    }
    

    # Fill in missing/non-matches with default value
    if(subs == "substances") { merged$idsubstance[which(is.na(merged$idsubstance))] <- 99999 }
    if(subs == "media") { merged$idmedia[which(is.na(merged$idmedia))] <- 99999 }
    message("Writing mapped data to temp table at: ", Sys.time())
    write_table_mmdb(name = 'temp_table', data = merged, con_type = con_type)

    message("Pushing data to ", tbl, " at: ", Sys.time())
    message("Pushing to database")
    if(con_type == "postgres"){
      query_map = paste0("UPDATE ", tbl, " h SET ", 
                         sub_list[[subs]]$id[[1]],"=m.", sub_list[[subs]]$id[[2]],
                         " FROM temp_table m ",
                         " WHERE h.", tbls[[tbl]]$id, "=m.", tbls[[tbl]]$id)
    } else {
      query_map = paste0("UPDATE ", tbl, ", temp_table SET ", 
                         tbl, ".", sub_list[[subs]]$id[[1]],"=temp_table.", sub_list[[subs]]$id[[2]],
                         " WHERE ", tbl, ".", tbls[[tbl]]$id, "=temp_table.", tbls[[tbl]]$id)
    }
    send_statement_mmdb(query_map, con_type = con_type)
    send_statement_mmdb("DROP TABLE IF EXISTS temp_table", con_type = con_type)
    
    message("Updated ", tbl, "... Moving on at: ", Sys.time())
  }
}
# Clear query results and disconnect
if (length(dbListResults(con))) {dbClearResult(dbListResults(con)[[1]])}; dbDisconnect(con); message("Done.")