## Script to generate variables related to a chemical's detected status
## Authors: Lindsay Eddy & Rachel Hencher
## Created:
## Last updated: 2024-08-12
## Assigns a detect flag to the harmonized data tables in the prod_samples database. 
## Columns added to the database's harmonized data tables:
## `detected` variable:         1  - chemical detected within sample
##                              0  - chemical not detected within sample
##                              NA - not enough info to determine detect/non-detect OR
##                                   row not processed (if the row was not processed,
##                                                      `detect_conflict` will be NA)
## `detect_conflict` variable:  1  - original source data is inconsistent/not clear 
##                                   whether measurement was a detect/non-detect
##                              0  - unambiguous detect/non-detect
##                              NA - row not processed
## `detect_note` variable:      Varies by source & situation

message("Running script to generate 'detected' variables at: ", Sys.time())
required.packages = c('DBI', 'RMySQL', 'RPostgreSQL', 'dplyr', 'dbplyr', 'data.table')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

############################################################
# Helper Functions
############################################################

#'@description A function to create a MySQL database connection from .Renviron parameters.
#'@return RMySQL connection.
connect_to_db = function(){
  return(dbConnect(RPostgreSQL::PostgreSQL(), 
                   user = Sys.getenv("postgres_user"), 
                   password = Sys.getenv("postgres_pass"),
                   host = Sys.getenv("postgres_host"),
                   dbname = Sys.getenv("postgres_dbname")))
}

#'@description A function mutate input dataframe by the specified conditions.
#'@param .data An input dataframe to mutate.
#'@param condition A boolean statement to filter the dataframe to.
#'@param ... Additional mutate parameters.
#'@return Modified input dataframe filtered to the condition parameter.
mutate_cond <- function(.data, condition, ..., envir = parent.frame()) {
  condition <- eval(substitute(condition), .data, envir)
  .data[condition, ] <- .data[condition, ] %>% mutate(...)
  .data
}

#'@description A function to get the database source table with datasource specific information.
#'@return A dataframe with datasource information.
get_sources <- function(){
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  sources = tbl(conn, "source") %>% collect()
  dbDisconnect(conn)
  return(sources)
}

#'@description A function to get the datasource ID from the source_name parameter.
#'@param source_name A string of the datasource's abbreviated name (e.g. ahhs, usgs).
#'@return An integer value for the datasource's ID value.
getsourceid <- function(source_name) {
  return(get_sources() %>%
           filter(source_name_abbr == source_name) %>%
           select(source_id))
}

#'@description A function to get the datasource name from the source_id parameter.
#'@param source_id An integer of the datasource's unique ID in the database.
#'@return A string value for the datasource's abbreviated name.
getsourcename <- function(sourceid) {
  return(get_sources() %>%
           filter(source_id == sourceid) %>%
           select(source_name_abbr))
}

#'@description A function to determine if a string can be converted to numeric.
#'@param str The input string to test.
#'@return A boolean.
is.number.string <- function(str) {
  # Returns a boolean - T if str can be converted to numeric (w/o NA result)
  return(!is.na(suppressWarnings(as.numeric(str))))
}

#'@description A function to determine if a string is an integer.
#'@param str The input string to test.
#'@return A boolean.
is.integer.string <- function(str) {
  # Returns a boolean - T if str is an integer
  return(is.number.string(str) & (ceiling(suppressWarnings(as.numeric(str))) == suppressWarnings(as.numeric(str))))
}

#'@description A function to conver to numeric, while suppressing the 'NAs introducted by coercion' warning.
#'@param x The object to convert to numeric.
#'@return An object converted to numeric.
as.numeric <- function(x, ...) {
  return(suppressWarnings(base::as.numeric(x, ...)))
}

############################################################
# Primary Functions
############################################################

#'@description A function to assign the detection flag (0-1) for an 'aggregate' datasource in the database
#'@return None. MySQL queries are pushed to the database.
assign.agg <- function() {

  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  # Get source table information.
  sources <- tbl(conn, "source") %>% collect()
  
  # Get the harmonized aggregate data information for the selected variables.
  message("Pulling aggregate data for all sources but epa_dmr at: ", Sys.time())
  
  harm_agg <- tbl(conn, "harmonized_aggregate") %>% 
    filter(source_id != 201) %>%
    select(
      harmonized_aggregate_id, source_id, reported_detect_rate, reported_n, 
      reported_num_nds, reported_nondetect_rate, reported_result, 
      reported_statistic, reported_num_detects, lod, loq, reported_chemical_name, 
      reported_media, reported_subpopulation, years, substance_id, media_id
    ) %>%
    collect()
  
  # Add three new variables to the harmonized aggregate data table for chemical detection status.
  harm_agg$detected <- NA_integer_
  harm_agg$detect_note <- NA_character_
  harm_agg$detect_conflict <- NA_integer_
  
  message("Assigning detect flags to aggregate data at: ", Sys.time())
  
  ## Begin: ahhs & ca_airmon
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("ahhs", getsourceid) |
        source_id %in% sapply("ca_airmon", getsourceid),
      detected = case_when(
        # First consider cases where the reported_result was reported as less than LOD.
        reported_result == "<MDL" ~ 0L,
        # A detection rate greater than 0 is a detect, a detection rate equal to 0 is a non-detect.
        as.numeric(reported_detect_rate) > 0 ~ 1L,
        as.numeric(reported_detect_rate) == 0 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0L,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(detected) & is.na(reported_result) ~ "No detection rate or result reported",
        is.na(detected) & !is.na(reported_result) ~ "Result reported with no LOD, LOQ, or detection rate"
      )
    )
  ## End: ahhs & ca_airmon
  
  ## Begin: biomon_ca
  # Some results are reported as a range or with special symbols, such as "%", which need stripped away prior to determining detected status. 
  # If a range is reported for LOD, the upper bound is used for determining detected status.
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("biomon_ca", getsourceid),
      detected = case_when(
        # First consider cases where the reported_result was reported as less than LOD.
        reported_result == "<LOD" ~ 0L,
        # Next, consider cases where the reported_detect_rate is given as 100% or 0%.
        (reported_detect_rate %>% {gsub("%", "", .)} %>% as.numeric()) == 100 ~ 1L,
        (reported_detect_rate %>% {gsub("%", "", .)} %>% as.numeric()) == 0 ~ 0L,
        # If the reported_detect_rate was between 0% & 100%, we consult whether it is greater than or less than the reported percentile, when given.
        (reported_detect_rate %>% {gsub("%", "", .)} %>% as.numeric()) >= (reported_statistic %>% {gsub("th", "", .)} %>% as.numeric()) ~ 1L,
        (reported_detect_rate %>% {gsub("%", "", .)} %>% as.numeric()) < (reported_statistic %>% {gsub("th", "", .)} %>% as.numeric()) ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      detect_conflict = case_when(
        # If the detected status is 1 but the reported_detect_rate is less than the percentile, then a conflict is present.
        detected == 1 & (reported_detect_rate %>% {gsub("%", "", .)} %>% as.numeric()) < (reported_statistic %>% {gsub("th", "", .)} %>% as.numeric()) ~ 1L,
        TRUE ~ 0L
      ),
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(reported_detect_rate) & is.na(reported_result) ~ "No detection rate or result reported",
        is.na(detected) & reported_result == "*" ~ "No detection rate or result reported: Geometric mean not calculated because the chemical was found in less than 65% of the study group"
      )
    )
  ## End: biomon_ca
  
  ## Begin: ctd
  # Some results are reported as a range or with special symbols, such as "%" & ">", which need stripped away prior to determining detected status.
  # If a range is reported for LOD, the upper bound is used for determining detected status.
  # If a range is reported for the result, the lower bound is used for determining detected status.
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("ctd", getsourceid),
      detected = case_when(
        # First consider situations where the detection rate has been reported. 
        # A detection rate greater than 0 is a detect, a detection rate equal to 0 is a non-detect, & a detection rate that includes a "-" or ">" is a detect.
        !is.na(reported_detect_rate) & grepl("-|>", reported_detect_rate) ~ 1L,
        !is.na(reported_detect_rate) & (reported_detect_rate %>% {gsub("%","",.)} %>% as.numeric()) > 0 ~ 1L,
        !is.na(reported_detect_rate) & (reported_detect_rate %>% {gsub("%","",.)} %>% as.numeric()) == 0 ~ 0L,
        # If the detection rate is was not reported, then we consult the reported result & compare to the LOD.
        is.na(reported_detect_rate) & (lod %>% {gsub("[^0-9.]","",.)} %>% as.numeric()) < as.numeric(reported_result) ~ 1L,
        is.na(reported_detect_rate) & (lod %>% {gsub("[^0-9.]","",.)} %>% as.numeric()) >= as.numeric(reported_result) ~ 0L,
        is.na(reported_detect_rate) & (lod %>% {gsub('.*-','',.)} %>% as.numeric()) < (reported_result %>% {gsub('-.*','',.)} %>% as.numeric()) ~ 1L,
        is.na(reported_detect_rate) & (lod %>% {gsub('.*-','',.)} %>% as.numeric()) >= (reported_result %>% {gsub('-.*','',.)} %>% as.numeric()) ~ 0L,
        is.na(reported_detect_rate) & paste0('<',lod) == reported_result ~ 0L,
        is.na(reported_detect_rate) & reported_result == "<LOD" ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0L,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(detected) & !is.na(reported_result) ~ "Result reported with no LOD, LOQ, or detection rate",
        is.na(detected) & is.na(reported_result) ~ "No detection rate or result reported"
      )
    )
  ## End: ctd
  
  ## Begin: carb
  # All rows are detects.
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("carb", getsourceid),
      # All rows are detects.
      detected = case_when(
        reported_result == 'NL, not listed' ~ NA_integer_,
        reported_result == '<LOQ' ~ NA_integer_,
        TRUE ~ 1
      ),
      # All reported number of detects are non-negative integers, so there is no conflicting data.
      detect_conflict = 0,
      # All rows are unambiguous detects.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        reported_result == '<LOQ' ~ "Result reported as < LOQ",
        TRUE ~ "No result listed"
      )
    )
  ## End: carb
  
  ## Begin: ip_chem_lakes
  # Detected when number of non-detects is less than the number of samples.
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("ip_chem_lakes", getsourceid),
      detected = case_when(
        as.numeric(reported_result) == 0 ~ 0L,
        # First consider situations where the reported n & number of non-detects have both been reported. Detected when number of non-detects is less than the number of samples.
        !is.na(reported_num_nds) & as.numeric(reported_num_nds) < as.numeric(reported_n) ~ 1L,
        !is.na(reported_num_nds) & as.numeric(reported_num_nds) == as.numeric(reported_n) ~ 0L,
        # If these were not reported, then we consult the reported result & compare to the LOQ.
        is.na(reported_num_nds) & as.numeric(reported_result) > as.numeric(loq) ~ 1L,
        is.na(reported_num_nds) & as.numeric(reported_result) == 0 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      detect_conflict = case_when(
        # More non-detects than total samples indicates an error in the original data.
        as.numeric(reported_num_nds) > as.numeric(reported_n) ~ 1L,
        TRUE ~ 0
      ),
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(detected) & as.numeric(reported_result) > 0 & as.numeric(reported_result) < as.numeric(loq) ~ "Reported result > 0, but < LOQ",
        is.na(detected) & !is.na(reported_result) ~ "Result reported with no LOD, LOQ, or detection rate"
      )
    ) %>%
    mutate_cond(
      # If either reported_n or reported_num_nds is NA, there is no conflicting data.
      source_id %in% sapply("ip_chem_lakes", getsourceid) & (is.na(reported_n) | is.na(reported_num_nds)),
      detect_conflict = 0
    )
  ## End: ip_chem_lakes
  
  ## Begin: epa_nscrlft
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("epa_nscrlft", getsourceid),
      # A reported number of detects greater than 0 is a detect, a reported number of detects equal to 0 is a non-detect.
      detected = case_when(
        as.numeric(reported_num_detects) > 0 ~ 1L,
        as.numeric(reported_num_detects) == 0 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # All reported number of detects are non-negative integers, so there is no conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: epa_nscrlft

  ## Begin: ip_chem_biomonitoring
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("ip_chem_biomonitoring", getsourceid),
      detected = case_when(
        as.numeric(reported_result) == 0 ~ 0L,
        # First consider situations where the detection rate has been reported.
        !is.na(reported_detect_rate) & as.numeric(reported_detect_rate) == 0 ~ 0L,
        !is.na(reported_detect_rate) & as.numeric(reported_detect_rate) > 0 ~ 1L,
        # If the detection rate is was not reported, then we consult the reported result & compare to the LOD/LOQ.
        is.na(reported_detect_rate) & as.numeric(reported_result) > as.numeric(lod) ~ 1L,
        is.na(reported_detect_rate) & as.numeric(reported_result) > as.numeric(loq) ~ 1L,
        is.na(reported_detect_rate) & as.numeric(reported_result) == 0 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # Reported result greater than LOD, but reported non-detect.
      detect_conflict = case_when(
        reported_detect_rate == 0 & 
          (as.numeric(reported_result) > as.numeric(lod)) ~ 1L,
        TRUE ~ 0
      ),
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(detected) & as.numeric(reported_result) > 0 & as.numeric(reported_result) < as.numeric(loq) ~ "Reported result > 0, but < LOQ",
        is.na(detected) & !is.na(reported_result) ~ "Result reported with no LOD, LOQ, or detection rate"
      )
    )
  ## End: ip_chem_biomonitoring
  
  ## Begin: nhanes
  harm_agg <- harm_agg %>%
    mutate_cond(
      source_id %in% sapply("nhanes", getsourceid),
      detected = case_when(
        # A reported result greater than the LOD indicates a detect.
        !is.na(reported_result) & as.numeric(reported_result) > 0 ~ 1L,
        !is.na(reported_result) & reported_result == "< LOD" ~ 0L,
        # Strip away values in parentheses to leave average value for determination of detect status
        !is.na(reported_result) & (reported_result %>% {gsub(" .*","",.)} %>% as.numeric()) > 0 ~ 1L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # All reported_num_detects are non-negative integers, so there is no conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(detected) & is.na(reported_result) ~ "No detection rate or result reported",
        is.na(detected) & reported_result == "*" ~ "No detection rate or result reported: Geometric mean not calculated because the chemical was found in less than 65% of the study group."
      )
    )
  ## End: nhanes
  
  cat(
    "Number of rows in harm_agg with updated detect_conflict: ",
    harm_agg %>% filter(!is.na(detect_conflict)) %>% nrow(),
    "/",
    harm_agg %>% nrow(),
    "\n"
  )
  
  # Pushes data into database.
  message("Writing detect flags to harmonized aggregate table...", Sys.time())
  
  harm_agg_update <- harm_agg %>%
    select(harmonized_aggregate_id, detected, detect_conflict, detect_note)
  
  dbWriteTable(conn = conn, name = "temp_harm_agg_update", value = harm_agg_update, row.names = F)
  
  query <- "UPDATE harmonized_aggregate
  SET detected = temp_harm_agg_update.detected,
  detect_conflict = temp_harm_agg_update.detect_conflict,
  detect_note = temp_harm_agg_update.detect_note
  FROM temp_harm_agg_update
  WHERE harmonized_aggregate.harmonized_aggregate_id = temp_harm_agg_update.harmonized_aggregate_id"
  dbSendStatement(conn, query)
  rm(query)
  
  query <- "DROP TABLE temp_harm_agg_update"
  dbSendStatement(conn, query)
  rm(query)
  
  dbDisconnect(conn)
}

#'@description A function to assign the detection flag (0-1) for the EPA DMR data source in the database. This source is so large,
#'it requires a special approach so the SQL queries work faster & R doesn't become memory overloaded.
#'@return None. RPostgreSQL queries are pushed to the database.
assign.epa_dmr <- function() {
  # We have to pull epa_dmr one file at a time, because if we try to pull all files at once, it will be too big to collect into a tibble.
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  # Get files table information.
  fileIDs <- tbl(conn, "files") %>% filter(source_id == 201) %>% select(file_id) %>% collect()
  fileIDs <- fileIDs$file_id
  
  for(f in fileIDs) {
    message("Pulling data for epa_dmr file ", f, " at: ", Sys.time())
    epa_dmr <- tbl(conn, "harmonized_aggregate") %>%
      select(
        harmonized_aggregate_id, source_id, file_id, reported_detect_rate, reported_n, 
        reported_result, reported_statistic, reported_chemical_name, 
        reported_subpopulation, years
      ) %>%
      filter(source_id == 201, file_id == f) %>%
      collect()
    
    epa_dmr$detected <- NA_integer_
    epa_dmr$detect_note <- NA_character_
    epa_dmr$detect_conflict <- NA_integer_
    
    message("Assigning detect flags to epa_dmr file ", f, " at: ", Sys.time())
    
    epa_dmr <- epa_dmr %>%
      mutate(
        # A reported result greater than 0 is a detect, a reported result equal to 0 is a non-detect.
        detected = case_when(
          as.numeric(reported_result) > 0 ~ 1L,
          as.numeric(reported_result) == 0 ~ 0L,
          # In cases where none of the previous conditions are met, assign NA for detected.
          TRUE ~ NA_integer_
        ),
        # All reported number of detects are non-negative integers, so there is no conflicting data.
        detect_conflict = 0,
        # Assign detect notes based on detected status.
        detect_note = case_when(
          detected == 1 ~ "Unambiguous detect",
          detected == 0 ~ "Unambiguous non-detect",
          is.na(detected) & is.na(reported_result) ~ "No detection rate or result reported"
        )
      )
    
    cat(
      "Number of rows in epa_dmr file ",
      f,
      " with updated detected status: ",
      epa_dmr %>% filter(!is.na(detected)) %>% nrow(),
      "/",
      epa_dmr %>% nrow(),
      "\n"
    )
    
    # Pushes data into database.
    dmr_update <- epa_dmr %>%
      select(harmonized_aggregate_id, detected, detect_conflict, detect_note)
    
    dbWriteTable(conn = conn, name = "temp_dmr_update", value = dmr_update, row.names = F)
    
    query <- "UPDATE harmonized_aggregate
    SET detected = temp_dmr_update.detected,
    detect_conflict = temp_dmr_update.detect_conflict,
    detect_note = temp_dmr_update.detect_note
    FROM temp_dmr_update
    WHERE harmonized_aggregate.harmonized_aggregate_id = temp_dmr_update.harmonized_aggregate_id"
    dbSendStatement(conn, query)
    rm(query)
    
    query <- "DROP TABLE temp_dmr_update"
    dbSendStatement(conn, query)
    rm(query)
  }
  dbDisconnect(conn)
}

#'@description A function to assign the detection flag (0-1) for a 'single-sample' datasource in the database.
#'@return None. MySQL queries are pushed to the database.
assign.raw <- function() {
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  # Get source table information
  sources <- tbl(conn, "source") %>% collect()
  
  # Get the harmonized raw data information for the selected variables.
  message("Pulling raw data for all sources but USGS at: ", Sys.time())
  
  harm_raw <- tbl(conn, "harmonized_raw") %>% 
    filter(source_id != 168) %>%
    select(
      harmonized_raw_id, source_id, reported_result, nd_flag, 
      lod, loq, result_flag, reported_chemical_name
    ) %>% 
    collect()
  
  harm_raw$detected <- NA_integer_
  harm_raw$detect_note <- NA_character_
  harm_raw$detect_conflict <- NA_integer_
  
  # Add detect flags & notes to each single-sample data source.
  message("Assigning detect flags to raw data at: ", Sys.time())
  
  ## Begin: ip_chem_ibs
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("ip_chem_ibs", getsourceid),
      # A reported result greater than the LOD indicates a detect.
      detected = case_when(
        as.numeric(reported_result) > as.numeric(lod) ~ 1L, 
        as.numeric(reported_result) <= as.numeric(lod) ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(detected) & !is.na(reported_result) ~ "Result reported with no LOD, LOQ, or detection rate",
        is.na(detected) & is.na(reported_result) ~ "No result reported"
      )
    )
  ## End: ip_chem_ibs
  
  
  ## Begin: ca_surf_water & ca_surf_sediment
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("ca_surf_water", getsourceid) |
        source_id %in% sapply("ca_surf_sediment", getsourceid),
      # A reported result greater than the LOD or LOQ indicates a detect.
      detected = case_when(
        as.numeric(reported_result) == 0 ~ 0L,
        as.numeric(reported_result) > as.numeric(lod) ~ 1L,
        as.numeric(reported_result) > as.numeric(loq) ~ 1L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        as.numeric(reported_result) > 0 & as.numeric(reported_result) <= as.numeric(loq) ~ "Result reported greater than 0, but not greater than LOQ",
        is.na(detected) & !is.na(reported_result) ~ "Result reported with no LOD, LOQ, or detection rate",
        is.na(reported_result) ~ "No result reported"
      )
    )
  ## End: ca_surf_water & ca_surf_sediment
  
  ## Begin: chem_theatre
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("chem_theatre", getsourceid),
      # A reported result equal to 0, "ND", or with a prefix of "<" are a non-detect. Other results greater than 0 are detects.
      detected = case_when(
        as.numeric(reported_result) > 0 ~ 1L,
        grepl("<", reported_result) | reported_result == "ND" ~ 0L,
        as.numeric(reported_result) == 0 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        reported_result == "-" ~ "No result reported",
        reported_result < 0 ~ "Reported result less than 0"
      )
    )
  ## End: chem_theatre
  
  ## Begin: epa_9potw
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("epa_9potw", getsourceid),
      # A non-detect flag of 0 is a detect, a non-detect flag of 1 is a non-detect.
      detected = case_when(
        as.numeric(nd_flag) == 0 ~ 1L,
        as.numeric(nd_flag) == 1 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 & reported_result == "DET" ~ "Detected but has associated QC flags",
        detected == 1 & reported_result != "DET" ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: epa_9potw
  
  ## Begin: epa_amtic
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("epa_amtic", getsourceid),
      # A non-detect flag of "Y" indicates a non-detect & the absence of a non-detect flag indicates a detect.
      detected = case_when(
        as.numeric(reported_result) == 0 ~ 0L,
        is.na(nd_flag) ~ 1L,
        nd_flag == "Y" ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # Reported result greater than LOD but reported as non-detect or reported result less than LOD but reported as detect.
      detect_conflict = case_when(
        !is.number.string(reported_result) ~ 0L,
        (as.numeric(reported_result) < as.numeric(lod)) & is.na(nd_flag) ~ 1L,
        (as.numeric(reported_result) > as.numeric(lod)) & (nd_flag == "Y") ~ 1L,
        TRUE ~ 0L
      ),
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: epa_amtic
  
  ## Begin: epa_tnsss
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("epa_tnsss", getsourceid),
      # A flag of "NC" indicates a detect & a flag of "ND" indicates a non-detect
      detected = case_when(
        nd_flag == "NC" ~ 1L,
        nd_flag == "ND" ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # A detect conflict exists when a non-detect flag is issued for a result > LOD.
      detect_conflict = case_when(
        !is.number.string(reported_result) | !is.number.string(lod) ~ 0L,
        (as.numeric(reported_result) <= as.numeric(lod)) & (nd_flag == "NC") ~ 1L,
        (as.numeric(reported_result) > as.numeric(lod)) & (nd_flag == "ND") ~ 1L,
        TRUE ~ 0L
      ),
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: epa_tnsss
  
  ## Begin: ices_biota & ices_sediment
  harm_raw <- harm_raw %>% 
    mutate_cond(
      source_id %in% sapply("ices_biota", getsourceid) |
        source_id %in% sapply("ices_sediment", getsourceid),
      detected = case_when(
        # First consider situations where are non-detect flag of "D" or "<~D" is given.
        nd_flag %in% c('D', '<~D') ~ 0L,
        # Next, we compare results between reported_result & the LOD.
        as.numeric(reported_result) > as.numeric(lod) ~ 1L,
        as.numeric(reported_result) <= as.numeric(lod) ~ 0L,
        as.numeric(reported_result) > as.numeric(loq) ~ 1L,
        as.numeric(reported_result) == 0 ~ 0L,
        # Additional non-detect flags should also be considered.
        nd_flag == '>' ~ 1L,
        nd_flag == '<' ~ 0L,
        is.na(lod) & is.na(loq) & as.numeric(reported_result) > 0 ~ 1L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # Conflicts occur wuth there is a non-detect flag, but the result is greater than the LOD.
      detect_conflict = case_when( 
        nd_flag %in% c('D', '<~D') &
          as.numeric(reported_result) > as.numeric(lod) ~ 1L,
        # Note that as.numeric(reported_result) is *never* <= as.numeric(lod) when nd_flag == '>'.
        TRUE ~ 0L
      ),
      # Assign detect notes based on detected status.
      detect_note = case_when(
        nd_flag == '>' & detected == 1 ~ "Ambiguous detect",
        nd_flag == '<' & is.na(lod) & detected == 0 ~ "Ambiguous non-detect",
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: ices_biota and ices_sediment
  
  ## Begin: fda_tds_elem
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("fda_tds_elem", getsourceid),
      # A reported result greater than the LOD indicates a detect.
      detected = case_when(
        as.numeric(reported_result) > as.numeric(lod) ~ 1L,
        as.numeric(reported_result) <= as.numeric(lod) ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: fda_tds_elem
  
  ## Begin: fda_tds_pest
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("fda_tds_pest", getsourceid),
      # A reported result greater than 0 indicates a detect.
      detected = case_when(
        as.numeric(reported_result) > 0 ~ 1L,
        as.numeric(reported_result) == 0 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(reported_result) ~ "No result reported"
      )
    )
  ## End: fda_tds_pest
  
  ## Begin: ip_chem_XXX (XXX = {biota, seawater, sediment})
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("ip_chem_biota", getsourceid) |
        source_id %in% sapply("ip_chem_seawater", getsourceid) |
        source_id %in% sapply("ip_chem_sediment", getsourceid),
      # A flag of '[' indicates sample below LOD; '<' indicates sample below LOQ
      detected = case_when(
        as.numeric(reported_result) == 0 ~ 0L,
        result_flag == "<" | is.na(result_flag) ~ 1L,
        result_flag == "[" ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        result_flag == "<" ~ "Result reported less than LOQ",
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: ip_chem_XXX
  
  ## Begin: usda_nrp
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("usda_nrp", getsourceid),
      # All observations are detects.
      detected = 1,
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect"
      )
    )
  ## End: usda_nrp
  
  ## Begin: epa_ucmr
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("epa_ucmr", getsourceid),
      # A flag of '=' indicates a detect and a flag of '<' indicates a non-detect
      detected = case_when(
        as.numeric(reported_result) == 0 ~ 0L,
        result_flag == "=" ~ 1L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        result_flag == '<' ~ "Result below minimum reporting level"
      )
    )
  ## End: epa_ucmr
  
  ## Begin: airmon
  harm_raw <- harm_raw %>%
    mutate_cond(
      source_id %in% sapply("airmon", getsourceid),
      # A reported result greater than the LOD indicates a detect
      detected = case_when(
        as.numeric(reported_result) > as.numeric(lod) ~ 1L,
        as.numeric(reported_result) <= as.numeric(lod) ~ 0L,
        as.numeric(reported_result) == 0 ~ 0L,
        # In cases where none of the previous conditions are met, assign NA for detected.
        TRUE ~ NA_integer_
      ),
      # None of the rows have conflicting data.
      detect_conflict = 0,
      # Assign detect notes based on detected status.
      detect_note = case_when(
        detected == 1 ~ "Unambiguous detect",
        detected == 0 ~ "Unambiguous non-detect",
        is.na(reported_result) ~ "No result reported",
        is.na(lod) & !is.na(reported_result) ~ "Result reported with no LOD"
      )
    )
  ## End: airmon
  
  cat(
    "Number of rows in harm_raw with updated detect_conflict: ",
    harm_raw %>% filter(!is.na(detect_conflict)) %>% nrow(),
    "/",
    harm_raw %>% nrow(),
    "\n"
  )
  
  # Pushes data into DB. Commented out until needed.
  message("Writing detect flags to harmonized raw table at: ", Sys.time())
  
  harm_raw_update <- harm_raw %>%
    select(harmonized_raw_id, detected, detect_conflict, detect_note)
  
  dbWriteTable(conn = conn, name = "temp_harm_raw_update", value = harm_raw_update, row.names = F)
  
  query <- "UPDATE harmonized_raw
  SET detected = temp_harm_raw_update.detected,
  detect_conflict = temp_harm_raw_update.detect_conflict,
  detect_note = temp_harm_raw_update.detect_note
  FROM temp_harm_raw_update
  WHERE harmonized_raw.harmonized_raw_id = temp_harm_raw_update.harmonized_raw_id"
  dbSendStatement(conn, query)
  rm(query)
  
  query <- "DROP TABLE temp_harm_raw_update"
  dbSendStatement(conn, query)
  rm(query)
  
  dbDisconnect(conn)
}

#'@description A function to assign the detection flag (0-1) for the USGS data source in the database. This source is so large,
#'it requires a special approach so the SQL queries work faster & R doesn't become memory overloaded.
#'@return None. MySQL queries are pushed to the database.
assign.usgs <- function() {
  # We have to pull USGS data one file at a time, because if we try to pull all files at once, it will be too big to collect into a tibble.
  
  # Connect to res_mmdb
  conn = connect_to_db()
  dbGetQuery(conn, "set search_path to mmdb")
  
  fileIDs <- tbl(conn, "files") %>% filter(source_id == 168) %>% select(file_id) %>% collect()
  fileIDs <- fileIDs$file_id
  
  for(f in fileIDs) {
    message("Pulling data for usgs file ", f, " at: ", Sys.time())
    usgs <- tbl(conn, "harmonized_raw") %>%
      select(
        harmonized_raw_id, source_id, file_id, nd_flag, 
        reported_result, result_flag, lod, loq, reported_chemical_name
      ) %>%
      filter(source_id == 168, file_id == f) %>%
      collect()
    
    usgs$detected <- NA_integer_
    usgs$detect_note <- NA_character_
    usgs$detect_conflict <- NA_integer_
    
    message("Assigning detect flags to usgs file ", f, " at: ", Sys.time())
    
    # Non-detect and detect lists
    ND_reported_result <- c("*Non-detect", "ND")
    
    ND_nd_flag <- c("Not Detected", "Present Below Quantification Limit", "Below Reporting Limit", "Below Method Detection Limit", 
                    "Below Long-term Blank-basd Dt Limit", "Below Detection Limit", "Not Present")
    D_nd_flag <- c("Detected Not Quantified", "Between Inst Detect and Quant Limit", "Present Above Quantification Limit", "Trace")
    
    ND_result_flag <- c("DL", "BMDL", "K")
    
    usgs <- usgs %>%
      mutate(
        detected = case_when(
          nd_flag == "Not Reported" ~ NA_integer_,
          # reported_result, nd_flag, or result_flag in non-detect list OR reported_result is of form "< number"
          reported_result %in% ND_reported_result |
            is.number.string(gsub("^<", "", reported_result)) & grepl("^<", reported_result) |
            nd_flag %in% ND_nd_flag |
            result_flag %in% ND_result_flag ~ 0L,
          # nd_flag in detect list OR positive reported result
          nd_flag %in% D_nd_flag |
            (is.number.string(reported_result) & as.numeric(reported_result) > 0) ~ 1L,
          as.numeric(reported_result) == 0 ~ 0L,
          TRUE ~ NA_integer_
        ),
        detect_conflict = case_when(
          nd_flag == "Not Reported" ~ 0L,
          # When a row has a variable in both a non-detect list AND a detect list, this is a detect conflict.
          (reported_result %in% ND_reported_result | nd_flag %in% ND_nd_flag | result_flag %in% ND_result_flag) &
            nd_flag %in% D_nd_flag ~ 1L,
          TRUE ~ 0L
        ),
        # Assign detect notes based on detected status.
        detect_note = case_when(
          detected == 1 ~ "Unambiguous detect",
          detected == 0 ~ "Unambiguous non-detect",
          nd_flag == "Not Reported" ~ "Data was collected but not analyzed",
          reported_result == "None" ~ "No result reported",
          is.na(reported_result) & is.na(nd_flag) ~ "No result or additional info reported",
          reported_result == "Present Below Quantification Limit" ~ "Result present but below quantification limit"
        )
      )
    
    cat(
      "Number of rows in usgs file ",
      f,
      " with updated detect_conflict: ",
      usgs %>% filter(!is.na(detect_conflict)) %>% nrow(),
      "/",
      usgs %>% nrow(),
      "\n"
    )
    
    # Pushes data into database
    usgs_update <- usgs %>%
      select(harmonized_raw_id, detected, detect_conflict, detect_note)
    
    dbWriteTable(conn = conn, name = "temp_usgs_update", value = usgs_update, row.names = F)
    
    query <- "UPDATE harmonized_raw
    SET detected = temp_usgs_update.detected,
    detect_conflict = temp_usgs_update.detect_conflict,
    detect_note = temp_usgs_update.detect_note
    FROM temp_usgs_update
    WHERE harmonized_raw.harmonized_raw_id = temp_usgs_update.harmonized_raw_id"
    dbSendStatement(conn, query)
    rm(query)
    
    query <- "DROP TABLE temp_usgs_update"
    dbSendStatement(conn, query)
    rm(query)
  }
  dbDisconnect(conn)
}

#'@description A function to run each of the detection assignment functions.
#'@return None. MySQL queries are pushed to the database.
assign.detects <- function() {
  #assign.agg()
  #assign.epa_dmr()
  assign.raw()
  #assign.usgs()
}

# Run the assignment function to run workflow. Remember to uncomment out the actual database push query lines.
assign.detects()

