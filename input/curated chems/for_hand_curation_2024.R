## Script to help curate chemicals by hand
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: Unknown
## Last updated: 2024-08-05

print("Running script to help curate chemicals...")
required.packages = c('dplyr', 'magrittr', 'readr', 'readxl', 'writexl')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

setwd("L:/Lab/CCTE_MMDB/MMDB/IT_MMDB/")

######################################################################
# Run prior to hand curation
######################################################################

# Read in list of chemicals for curation
stop("Before proceeding, update input file path to utilize the file with the most recent date")
chems <- read_xlsx("input/curated chems/chems_to_curate_YYYY-MM-DD.xlsx")

# Read in list of chemicals already curated
map <- read_csv("input/curated chems/forhandcurationofnonchemicals_completed.csv", col_types = cols()) %>% select(source_name_abbr, reported_chemical_name, CHEMICAL)
# Set a default value of '0' for any substances where chemical status is unknown
map$CHEMICAL[is.na(map$CHEMICAL)] <- 0

mapped_chems <- left_join(chems, map, by = c("source_name_abbr", "reported_chemical_name"))

output <- mapped_chems %>%
  mutate(withoutparenthetical = gsub("\\s*\\([^\\)]+\\)", "", reported_chemical_name) %>% 
           gsub("[\r\n]", "", .) %>% 
           trimws(),# Remove parenthetical completely
         parenthetical = stringr::str_extract(string = reported_chemical_name,
                                              pattern = "(?<=\\().*(?=\\))") %>% 
           gsub("[\r\n]", "", .) %>%
           trimws(), # Extract parenthetical
         reported_chemical_name = trimws(reported_chemical_name)
         )
# Designate those substances with reported_casrn as chemicals
output$CHEMICAL[!is.na(output$reported_casrn)] <- 1

write_xlsx(output, paste0("input/curated chems/for_hand_curation_of_nonchemicals_", Sys.Date(), ".xlsx"))

######################################################################
# Run after hand curation has been completed
######################################################################

# Filter file of chemicals for hand curation after NA "CHEMICAL" indicator fields have been curated
# Write a list of chemicals where curation has been completed
chems <- read_xlsx("input/curated chems/for_hand_curation_of_nonchemicals_2024-08-06.xlsx") %>%
  filter(CHEMICAL == 1) %T>%
write_xlsx(., paste0("input/curated chems/for_hand_curation_of_nonchemicals_", Sys.Date(), "_completed.xlsx"))
