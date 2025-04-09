## Script to pull unique media information from harmonized aggregate and harmonized raw tables.
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: Unknown
## Last updated: 2024-07-31

print("Running script to pull all unique media...")
required.packages = c('DBI', 'dplyr', 'magrittr')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(RPostgreSQL::PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

con <- connect_to_mmdb
dbGetQuery(con, "set search_path to mmdb")

# Pull media & data source ID data from harmonized tables
unique_media <- lapply(c("harmonized_aggregate", "harmonized_raw"), function(x){
  tbl(con, x) %>% 
    select(source_id, reported_media, reported_species) %>% 
    distinct() %>% 
    collect()
}) %>% bind_rows()

# Pull source & media table info
source <- tbl(con, "source") %>% collect()
curated_media <- tbl(con, "media") %>% select(source_id, reported_media, reported_species) %>% collect()

dbDisconnect(con)

# Save original copy of all unique media
unique_media <- unique_media %>%
  left_join(source, by = "source_id") %>% 
  select(source_id, reported_media, reported_species) %>%
  mutate(across(c("reported_media","reported_species"), ~trimws(tolower(.x)))) %T>% {
    write_csv(., paste0("input/media mapping/unique_media_list_", Sys.Date(), ".csv"))
  }

# Filter out curated media to save as a list of media in need of curation
output <- anti_join(unique_media, curated_media,
                   by = c("source_id", "reported_media", "reported_species")) %T>% { 
                     write_csv(., paste0("input/media mapping/unmapped_media_", Sys.Date(), ".csv")) }