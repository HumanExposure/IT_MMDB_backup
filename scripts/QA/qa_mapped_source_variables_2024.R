## Script to QA variable mapping between raw source files and harmonized tables
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: 2020-04-13
## Last updated: 2024-07-31

message("Running script to QA variable mapping...")
required.packages = c('DBI', 'dplyr', 'magrittr', 'tidyr', 'compareDF')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(RPostgreSQL::PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

con <- connect_to_mmdb
dbGetQuery(con, "set search_path to mmdb")

# Create ID table for data sources
id_table <- left_join(tbl(con, "source") %>% select(source_id, source_name_abbr, data_collection_type), 
                      tbl(con, "files") %>% select(-mapped, -harm_init), 
                      by = c("source_id" = "source_id")) %>%
  collect() %>%
  mutate(src_id = paste(source_name_abbr, file_id, sep = "_"))

# Get list of map to and from variables
source_dir <- "L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/"
# Limit database pull size to check
pull_size <- 10

# Pull and format database field map file
map_name <- "input/map_all_variables_2024-07-26.csv"
map <- read.csv(paste0(map_name), stringsAsFactors = F)
names(map) <- tolower(names(map))
map <- filter(map, to != "") %>%
  mutate_at(vars("to", "from"), list(~gsub('[[:punct:] ]+', ' ', trimws(tolower(.))))) %>% # Standardize the "from" column for raw fields
  arrange(source, to) # Order by data source and 'to' column

# Pull directly from raw tables, convert wide form, select mapped variables in order
# Only pull pull size number of rows
system.time({ # Measure time passed because of long raw table query time
  file_list <- id_table$file_id
  record_list <- seq_len(pull_size)
  raw_file_data <- lapply(file_list, function(f){
    message("Pulling file: ", f)
    type <- id_table$data_collection_type[id_table$file_id == f]
    sourceID <- id_table$source_id[id_table$file_id == f]
    src_name <- id_table$source_name_abbr[id_table$file_id == f] %>% unique()
    tmp = tbl(con, paste0("raw", ifelse(type == "aggregate", "_aggregate", ""), "_data")) %>% 
      filter(file_id == f, source_id == sourceID, record_id %in% record_list) %>%
      select(variable_name, variable_value, file_id, record_id, source_id) %>%
      collect() %>%
      spread(key = variable_name, value = variable_value) %T>% { 
        names(.) <- gsub('[[:punct:] ]+', ' ', trimws(tolower(names(.)))) } %>% 
      select(any_of(map$from[map$source == src_name][map$from[map$source == src_name] %in% names(.)])) %>%
      mutate(across(all_of(names(.)), na_if, ""),
             across(all_of(names(.)), na_if, " "),
            #across(all_of(names(.)), na_if, "NA"), # Removed for now due to elemental sodium
             across(all_of(names(.)), na_if, "<NA>"),
             across(all_of(names(.)), na_if, "<na>"))
  })
  names(raw_file_data) <- id_table$src_id # Name list
})

# Pull directly from harmonized tables, select mapped variables in order
# Only pull pull_size number of rows
system.time({
  record_list <- seq_len(pull_size)
  harmonized_file_data <- lapply(file_list, function(f){
    message("Pulling file: ", f)
    type <- id_table$data_collection_type[id_table$file_id == f]
    sourceID <- id_table$source_id[id_table$file_id == f]
    src_name <- id_table$source_name_abbr[id_table$file_id == f] %>% unique()
    tmp = tbl(con, paste0("harmonized_", type)) %>% 
      filter(file_id == f, source_id == sourceID, record_id %in% record_list) %>%
      collect() %T>% { 
        names(.) <- gsub('[[:punct:] ]+', ' ', trimws(tolower(names(.)))) } %>% 
      select(any_of(map$to[map$source == src_name][map$to[map$source == src_name] %in% names(.)])) %>%
      mutate(across(all_of(names(.)), na_if, ""),
             across(all_of(names(.)), na_if, " "),
            #across(all_of(names(.)), na_if, "NA"), # Removed for now due to elemental sodium
             across(all_of(names(.)), na_if, "<NA>"),
             across(all_of(names(.)), na_if, "<na>"))
  })
  names(harmonized_file_data) <- id_table$src_id # Name list
})
dbDisconnect(con) # Disconnect from database since all data is pulled

# Compare if the data matches (fixed empty entries)
# Need to fix types mismatches?
comparision_stats <- lapply(names(raw_file_data), function(x){
  if(is.null(raw_file_data[[x]])) return(NULL)
  temp <- compare_df(df_old = raw_file_data[[x]] %>% 
                       mutate_if(is.character, list(~na_if(., ""))) %T>%
                       { names(.) <- seq_len(length(names(.)))} %>%
                       tibble::rowid_to_column(), 
                     df_new = harmonized_file_data[[x]] %>%
                       mutate_if(is.character, list(~na_if(., ""))) %T>%
                       { names(.) <- seq_len(length(names(.)))} %>%
                       tibble::rowid_to_column(), 
                     group_col = "rowid", 
                     stop_on_error = F)
  # Save GitHub-like comparison table HTML file
  create_output_table(temp, file_name = paste0("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/output/comparison tables/", x, ".html"))
  return(temp$change_summary)  
}) %T>% { names(.) <- names(raw_file_data) }
