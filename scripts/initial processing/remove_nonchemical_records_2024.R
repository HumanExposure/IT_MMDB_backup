## A script to remove MMDB harmonized table records for non-chemicals
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: 2021-02-18
## Last updated: 2024-08-13

message("Running script to remove non-chemicals...")
required.packages = c('DBI', 'dplyr', 'readr')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(RPostgreSQL::PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

con <- connect_to_mmdb
dbGetQuery(con, "set search_path to mmdb")

# Pulling QA'd list of non-chemicals - Pulled from unique list of chemicals with substance ID 9999
stop("Before proceeding, update input file path to utilize the file with the most recent date")
nonchems = read_csv("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/curated chems/nonchemical_record_QA_YYYY-MM-DD.csv", col_types = cols()) %>%
  filter(is.na(Chemical)| Chemical != 1) %>%
  select(reported_chemical_name, reported_casrn, source_id)

# Create ID table with data source and file IDs
src_table <- tbl(con, "source") %>% rename_all(function(x) paste0("src.", x)) %>% collect()
id_table <- left_join(tbl(con, "source") %>% select(source_id, source_name_abbr, data_collection_type), 
                     tbl(con, "files") %>% select(source_id, file_id), 
                     by = "source_id") %>%
  collect() %>%
  filter(source_id %in% unique(nonchems$source_id)) # Filter out IDs not present in QA'd list
if(!dir.exists("input/non-chemical records removed")){ # Create directory to log removed file records
  message("Creating non-chemical records removed log directory")
  dir.create("input/non-chemical records removed")
}
# Loop through all database files
for(f in id_table$file_id){
  # Pull identifiers to pull data by
  tbl <- paste0("harmonized_", id_table$data_collection_type[id_table$file_id == f])
  src <- id_table$source_id[id_table$file_id == f]
  harmonizedID = ifelse(tbl == "harmonized_aggregate", "harmonized_aggregate_id", "harmonized_raw_id")
  message("Prepping chemicals to remove...")
  # Filter to relevant non-chemicals identified for removal
  rm_chems = nonchems %>%
    filter(source_id == src) %>%
    select(reported_chemical_name, reported_casrn) %>%
    mutate(reported_chemical_name = tolower(trimws(reported_chemical_name)) %>% gsub("\\s+", " ", .)) # Standardize for matching purposes
  # Harmonize missing just in case
  rm_chems$reported_casrn[rm_chems$reported_casrn %in% c("", " ", "NA", "na", "<NA>", "<na>", "NULL", "null")] <- NA # Replace empty values with NA
  rm_chems$reported_chemical_name[rm_chems$reported_chemical_name %in% c("", " ", "<NA>", "<na>", "NULL", "null")] <- NA # Replace empty values with NA  
  # Create column to match by
  rm_chems = mutate(rm_chems, match = paste(reported_chemical_name, reported_casrn, sep = "_"))
  message("Pulling harmonized data...")
  # Pull harmonized file data
  rawvals <- tbl(con, tbl) %>% 
    filter(source_id == src, file_id == f) %>% 
    select(all_of(harmonizedID), source_id, file_id, record_id, reported_chemical_name, reported_casrn) %>% 
    distinct() %>%
    collect() %>%
    mutate(reported_chemical_name = tolower(trimws(reported_chemical_name)) %>% gsub("\\s+", " ", .)) # Standardize for matching purposes
  # Harmonize missing just in case
  rawvals$reported_casrn[rawvals$reported_casrn %in% c("", " ", "NA", "na", "<NA>", "<na>", "NULL", "null")] <- NA # Replace empty values with NA
  rawvals$reported_chemical_name[rawvals$reported_chemical_name %in% c("", " ", "<NA>", "<na>", "NULL", "null")] <- NA # Replace empty values with NA  
  # Filter to matches
  rawvals = rawvals %>%
    mutate(match = paste(reported_chemical_name, reported_casrn, sep = "_")) %>%
    filter(match %in% rm_chems$match)
  if(nrow(rawvals)){# Check if any non-chemical records matched
    if(!file.exists(paste0("input/non-chemical records removed/removed_log_", f, "_", Sys.Date(), ".csv"))){
      message("Creating log of non-chemical records removed for today's run")
      write_csv(x = data.frame(recordTime = as.character(), harmonized_id = as.character(), 
                               source_id = as.character(), file_id = as.character(),
                               record_id = as.character(), reported_chemical_name = as.character(),
                               reported_casrn = as.character(), match = as.character()), 
                paste0("input/non-chemical records removed/removed_log_", f, "_", Sys.Date(), ".csv"))
    }
    message("Logging records to be removed")
    if(file.exists(paste0("input/non-chemical records removed/removed_log_", f, "_", Sys.Date(), ".csv"))){
      write.table(data.frame(recordTime = Sys.time(), harmonized_aggregate_id = rawvals[harmonizedID], 
                             source_id = rawvals$source_id, file_id = rawvals$file_id,
                             record_id = rawvals$record_id, reported_chemical_name = rawvals$reported_chemical_name,
                             reported_casrn = rawvals$reported_casrn, match = rawvals$match), 
                  paste0("input/non-chemical records removed/removed_log_", f, "_", Sys.Date(), ".csv"), 
                  sep = ",", append = T, row.names = F, col.names = FALSE)
      message("...Appended to existing log")
    } else {
      stop("Master log file not found, cannot log worksheet entry")
    }
    message("Removing non-chemical records from file ", f," (", which(id_table$file_id == f), " of ", length(id_table$file_id), ") at ", Sys.time())  
    message("...Deleting records from ", tbl, " at: ", Sys.time())
    dbSendStatement(con,
                    paste0("DELETE FROM ", tbl, 
                           " WHERE ", tbl, ".", harmonizedID, " IN (", paste(rawvals[[harmonizedID]], collapse = ", "), ")")
    )
    message("...Updated ", tbl, "...moving on...", Sys.time())
  } else {
    message("No matching non-chemical records found for file ", f, "...moving on...")
  }
}
dbDisconnect(con)
message("Done at: ", Sys.time())