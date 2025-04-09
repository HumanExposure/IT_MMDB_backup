## Script to pull unique chemical substance information (reported name and reported CASRN) from harmonized aggregate and harmonized raw tables.
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: Unknown
## Last updated: 2024-07-31

print("Running script to pull all unique substances...")
required.packages = c('DBI', 'dplyr', 'magrittr', 'readr', 'writexl')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(RPostgreSQL::PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

con <- connect_to_mmdb
dbGetQuery(con, "set search_path to mmdb")

# Pull chemical & data source ID data from harmonized tables
unique_chems <- lapply(c("harmonized_raw", "harmonized_aggregate"), function(x){
  tbl(con, x) %>% 
    select(source_id, reported_chemical_name, reported_casrn) %>% 
    group_by(source_id) %>% 
    distinct() %>% 
    collect()
}) %>% bind_rows()

# Pull source & substance table info
source <- tbl(con, "source") %>% collect()
chems <- tbl(con, "substances") %>% collect()

dbDisconnect(con)

# Get current substances table to filter to uncurated
curated_chems <- chems  %>%
  left_join(source, by = c("source_id" = "source_id")) %>%
  select(source_id, source_name_abbr, reported_chemical_name, reported_casrn) %>%
  distinct()

unique_chems <- unique_chems %>%
  ungroup() %>%
  left_join(source, by = c("source_id" = "source_id")) %>%
  select(source_id, source_name_abbr, reported_chemical_name, reported_casrn)

# Save original copy of all unique chemicals
write_csv(unique_chems, paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/curated chems/all_unique_chems_", Sys.Date(), ".csv"))

# Fix erroneous conversion to date format that happens with CSV files --> Rearrange to correct format
if(length(unique_chems$reported_casrn[grepl("/", unique_chems$reported_casrn)])){
  unique_chems$reported_casrn[grepl("/", unique_chems$reported_casrn)] <- sapply(strsplit(unique_chems$reported_casrn[grepl("/", unique_chems$reported_casrn)], "/"), 
                                                                          function(x){
                                                                            if(as.numeric(x[1]) < 10){ x[1] <- paste0(0, x[1]) 
                                                                          }
    return(paste(x[3], x[1], x[2], sep = "-"))
  })
}

# Fix erroneous conversion to date format that happens with CSV files --> Rearrange to correct format
if(length(curated_chems$reported_casrn[grepl("/", curated_chems$reported_casrn)])){
  curated_chems$reported_casrn[grepl("/", curated_chems$reported_casrn)] <- sapply(strsplit(curated_chems$reported_casrn[grepl("/", curated_chems$reported_casrn)], "/"), 
                                                                                 function(x){
                                                                                   if(as.numeric(x[1]) < 10){ x[1] <- paste0(0, x[1]) 
                                                                                 }
    return(paste(x[3], x[1], x[2], sep = "-"))
  })
}

# Save original copy of all unique chemicals with CASRN fixes
write_xlsx(unique_chems, paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/curated chems/all_unique_chems_casrn_fix_", Sys.Date(), ".xlsx"))

# Filter out curated chems to save as a list of chemicals in need of curation
output <- anti_join(unique_chems %>% mutate(reported_chemical_name = trimws(tolower(reported_chemical_name))),
                   curated_chems, by = c("source_id", "reported_chemical_name", "reported_casrn"))

write_xlsx(output, paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/curated chems/chems_to_curate_", Sys.Date(), ".xlsx"))