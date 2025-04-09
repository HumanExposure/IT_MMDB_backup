## A script to QA MMDB harmonized table for non-chemicals records (substance_id = 99999)
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: 2021-03-05
## Last updated: 2024-08-13

message("Running script to QA non-chemical records...")
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


# Source and substance table pointers
src <- tbl(con, "source") %>% rename_all(function(x) paste0("src.", x))
s <- tbl(con, "substances") %>% rename_all(function(x) paste0("s.", x))

# Harmonized table names to pull
tbls <- c("harmonized_aggregate", "harmonized_raw")
lapply(tbls,
       function(x){# Join tables, filter to 99999, select columns, save intermediate, save output file
         data <- tbl(con, x) %>% rename_all(function(y) paste0("h.", y)) %>%
           left_join(src, by = c("h.source_id" = "src.source_id")) %>%
           left_join(s, by = c("h.substance_id" = "s.substance_id")) %>%
           filter(h.substance_id == 99999) %>%
           select(h.reported_chemical_name, h.reported_casrn,
                  h.source_id, src.source_name_abbr) %>%
           mutate(Chemical = NA) %>%
           collect() %T>%{ # Important T-operator
             colnames(.) = sub('.*\\.', '', colnames(.)) # Remove prefixes
           }
       }) %>% bind_rows() %T>% {
         . ->> output; # Save intermediate
         readr::write_csv(., paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/input/curated chems/nonchemical_record_QA_", Sys.Date(), ".csv"))
       }
dbDisconnect(con)