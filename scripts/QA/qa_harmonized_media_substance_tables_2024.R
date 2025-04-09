## Script to QA variable media & substances for harmonized tables
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created:
## Last updated: 2024-08-13

print("Running script to QA media & substances tables...")
required.packages = c('DBI', 'plyr', 'dplyr', 'magrittr', 'tidyr', 'reshape')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

conn <- dbConnect(RPostgreSQL::PostgreSQL(), 
                  user = Sys.getenv("postgres_user"), 
                  password = Sys.getenv("postgres_pass"),
                  host = Sys.getenv("postgres_host"),
                  dbname = Sys.getenv("postgres_dbname"))

dbGetQuery(conn, "set search_path to mmdb")

# Create a datasource ID table
id_table <- left_join(tbl(conn, "source") %>% select(source_id, source_name_abbr, data_collection_type), 
                     tbl(conn, "files") %>% select(-mapped, -harm_init), 
                     by = "source_id") %>%
  collect() %>%
  mutate(srcID = paste(source_name_abbr, file_id, sep = "_"))

# QA Substances Table
src <- tbl(conn, "source") %>% rename_all(function(x) paste0("src.", x))
s <- tbl(conn, "substances") %>% rename_all(function(x) paste0("s.", x))
ha <- tbl(conn, "harmonized_aggregate") %>% rename_all(function(x) paste0("ha.", x))
hr <- tbl(conn, "harmonized_raw") %>% rename_all(function(x) paste0("hr.", x))
m <- tbl(conn, "media") %>% rename_all(function(x) paste0("m.", x))

# Aggregate Table - by file ID
sub_agg_QA <- lapply(id_table$file_id[id_table$data_collection_type == "aggregate"], function(x){
  source_id = id_table$source_id[id_table$file_id == x]
  return(
    ha %>%
      filter(ha.source_id == source_id, ha.file_id == x) %>%
      left_join(src, by = c("ha.source_id" = "src.source_id")) %>%
      left_join(s, by = c("ha.substance_id" = "s.substance_id")) %>%
      select(src.source_name_abbr, substance_id = ha.substance_id, ha.reported_chemical_name, ha.reported_casrn,
             s.dtxsid, s.reported_chemical_name, s.reported_casrn, s.preferred_name, s.casrn) %>%
      group_by(src.source_name_abbr, ha.reported_chemical_name) %>%
      distinct() %>%
      collect()
  )
})
names(sub_agg_QA) <- id_table$file_id[id_table$data_collection_type == "aggregate"]
total_agg_sub <- sub_agg_QA %>% bind_rows() %>% distinct()

# Raw Table - by file ID
system.time({
  sub_raw_QA <- lapply(id_table$file_id[id_table$data_collection_type == "raw"], function(x){
    source_id = id_table$source_id[id_table$file_id == x]
    return(
      hr %>%
        filter(hr.source_id == source_id, hr.file_id == x) %>%
        left_join(src, by = c("hr.source_id" = "src.source_id")) %>%
        left_join(s, by = c("hr.substance_id" = "s.substance_id")) %>%
        select(src.source_name_abbr, substance_id = hr.substance_id, hr.reported_chemical_name, hr.reported_casrn,
               s.dtxsid, s.reported_chemical_name, s.reported_casrn, s.preferred_name, s.casrn) %>%
        group_by(src.source_name_abbr, hr.reported_chemical_name) %>%
        distinct() %>%
        collect()
    )
  })
  names(sub_raw_QA) <- id_table$file_id[id_table$data_collection_type == "raw"]
})
total_raw_sub <- sub_raw_QA %>% bind_rows() %>% distinct()

# QA Media Table by file ID
# Aggregate Table
med_agg_QA <- lapply(id_table$file_id[id_table$data_collection_type == "aggregate"], function(x){
  source_id = id_table$source_id[id_table$file_id == x]
  return(
    ha %>%
      filter(ha.source_id == source_id, ha.file_id == x) %>%
      left_join(src, by = c("ha.source_id" = "src.source_id")) %>%
      left_join(m, by = c("ha.media_id" = "m.media_id")) %>%
      select(src.source_name_abbr, ha.reported_chemical_name, media_id = ha.media_id, ha.reported_media, ha.reported_species,
             m.media_id = ha.media_id, m.reported_media, m.reported_species, m.harmonized_medium) %>%
      group_by(src.source_name_abbr, ha.reported_chemical_name) %>%
      distinct() %>%
      collect()
  )
})
names(med_agg_QA) <- id_table$file_id[id_table$data_collection_type == "aggregate"]
total_agg_med <- med_agg_QA %>% bind_rows() %>% distinct()

# Raw Table
system.time({
  med_raw_QA <- lapply(id_table$file_id[id_table$data_collection_type == "raw"], function(x){
    source_id = id_table$source_id[id_table$file_id == x]
    return(
      hr %>%
        filter(hr.source_id == source_id, hr.file_id == x) %>%
        left_join(src, by = c("hr.source_id" = "src.source_id")) %>%
        left_join(m, by = c("hr.media_id" = "m.media_id")) %>%
        select(src.source_name_abbr, hr.reported_chemical_name, media_id = hr.media_id, hr.reported_media, hr.reported_species,
               m.media_id = hr.media_id, m.reported_media, m.reported_species, m.harmonized_medium) %>%
        group_by(src.source_name_abbr, hr.reported_chemical_name) %>%
        distinct() %>%
        collect()
    )
  })
  names(med_raw_QA) <- id_table$file_id[id_table$data_collection_type == "raw"]
})
total_raw_med <- med_raw_QA %>% bind_rows() %>% distinct()

# Create XLSX output with each sheet as aggregate/raw type for substances or media
# Use for manual QA review
writexl::write_xlsx(x = list(agg_sub = total_agg_sub,
                             agg_med = total_agg_med,
                             raw_sub = total_raw_sub,
                             raw_med = total_raw_med),
                    path = paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/output/substances_media_table_QA_", Sys.Date(), ".xlsx"))

dbDisconnect(conn)

# Generate aggregate substances summary
agg_sum <- total_agg_sub %>%
  group_by(src.source_name_abbr) %T>%{
    summarise(., uniqueChems = n()) ->> tmp # Save intermediate to combine back in
  } %>%
  filter(!is.na(s.dtxsid)) %>%
  summarise(uniqueDTXSID = n()) %>%
  left_join(tmp, by = "src.source_name_abbr") %>%
  left_join(total_agg_med %>%
              group_by(src.source_name_abbr) %>%
              summarise(uniqueMedia = paste0(unique(m.harmonized_medium), collapse = "; ")),
            by = "src.source_name_abbr")

# Generate raw substances summary
raw_sum <- total_raw_sub %>%
  group_by(src.source_name_abbr) %T>%{
    summarise(., uniqueChems = n()) ->> tmp # Save intermediate to combine back in
  } %>%
  filter(!is.na(s.dtxsid)) %>%
  summarise(uniqueDTXSID = n()) %>%
  left_join(tmp, by = "src.source_name_abbr") %>%
  left_join(total_raw_med %>%
              group_by(src.source_name_abbr) %>%
              summarise(uniqueMedia = paste0(unique(m.harmonized_medium), collapse = "; ")),
            by = "src.source_name_abbr")

# Write a summary report
writexl::write_xlsx(x = list(harm_agg = agg_sum,
                             harm_raw = raw_sum),
                    path = paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/output/harmonized_table_summaries_", Sys.Date(), ".xlsx"))

message("Done at: ", Sys.time())