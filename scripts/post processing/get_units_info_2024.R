## Script to pull units information from harmonized aggregate and harmonized raw tables.
## Authors: Rachel Hencher
## Created: 2024-10-16
## Last updated: 2024-10-16

print("Running script to pull units information...")
required.packages = c('DBI', 'dplyr', 'magrittr')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(RPostgreSQL::PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

conn <- connect_to_mmdb
dbGetQuery(conn, "set search_path to mmdb")

# Pull relevant information from aggregate and raw harmonized tables and bind together
AGGREGATE <- tbl(conn, "harmonized_aggregate") %>%
  select(source_id, reported_units, cleaned_units) %>%
  group_by(source_id, reported_units, cleaned_units) %>%
  summarise(n = n()) %>%
  collect()

RAW <- tbl(conn, "harmonized_raw") %>%
  select(source_id, reported_units, cleaned_units) %>%
  group_by(source_id, reported_units, cleaned_units) %>%
  summarise(n = n()) %>%
  collect()

unit_info <- rbind(AGGREGATE, RAW)

# Write to a csv file stored in the repo's input folder
write.csv(unit_info, paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/cleaned units/units_", Sys.Date(), ".csv"), row.names = F, fileEncoding = "UTF-8")

# The user will manually fill in any blanks for "cleaned_units" column in the csv file generated above prior to running the cleaned_units_var_2024.R script