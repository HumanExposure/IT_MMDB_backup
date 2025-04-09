## Script to update media table with media data
## Authors: Krisitn Isaacs, Jonathan Taylor Wall, & Rachel Hencher
## Created: Unknown
## Last updated: 2024-08-07

print("Running script to update the substances table...")
required.packages = c('dplyr', 'magrittr', 'tidyr', 'readr', 'tibble')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

con <- connect_to_mmdb
dbGetQuery(con, "set search_path to mmdb")

# Pull source list
source <- tbl(con, "source") %>% select(source_id, source_name_abbr) %>% collect()

stop("Before proceeding, update input file paths for media list & media crosswalk to utilize the file with the most recent date")

# Pull unique media list
unique_media <- read_csv("input/media mapping/unique_media_list_YYYY-MM-DD.csv", col_types = cols())
unique_media$reported_media <- tolower(unique_media$reported_media)
unique_media$reported_species <- tolower(unique_media$reported_species)

# Pull mapped media list
crosswalk <- read_csv("input/media crosswalk/master_media_crosswalk_YYYY-MM-DD.csv", col_types = cols())

# Map unique media
mapped <- left_join(unique_media, crosswalk, by = c("source_id", "reported_media", "reported_species"))

# Check if any missing media were not mapped or dropped unnecessarily
test <- anti_join(mapped, unique_media)
test2 <- anti_join(unique_media, mapped)

if(nrow(test) | nrow(test2)){ stop("Error: Unmapped media present... Check media mapping...") }

# Selecting distinct and renumbering
media <- mapped %>% 
  distinct() %>%
  arrange(source_id, harmonized_media) %>%
  rowid_to_column(var = "media_id") %>%
  rename(harmonized_medium = "harmonized_media")

media <- rbind(media, #Adding fields for media table
               data.frame(source_id = NA, media_id = 99999, reported_media = NA, 
                          reported_species = NA, harmonized_medium = "unknown", stringsAsFactors = F)) %>%
  left_join(source, by = "source_id") %>%
  select(-source_name_abbr)

# Push to database
dbWriteTable(con, name = 'temp_table3', value = media, row.names = F, overwrite = T)
dbSendStatement(con, paste0("INSERT INTO media (", toString(names(media)), ") SELECT ",
                            toString(names(media)), " FROM temp_table3"))

# Clear query results and disconnect
if (length(dbListResults(con))) {dbClearResult(dbListResults(con)[[1]])}; dbDisconnect(con); message("Done...")
