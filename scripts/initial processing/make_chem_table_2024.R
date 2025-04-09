## Script to update substances table with substance data
## Authors: Krisitn Isaacs, Jonathan Taylor Wall, & Rachel Hencher
## Created: Unknown
## Last updated: 2024-08-06

print("Running script to update the substances table...")
required.packages = c('dplyr', 'magrittr', 'lubridate', 'readr')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

con <- connect_to_mmdb
dbGetQuery(con, "set search_path to mmdb")

# Pull source table data to match for source_id field
source <- tbl(con, "source") %>% select(source_id, source_name_abbr) %>% collect()

# Pulling curated substance data file
stop("Before proceeding, update input file path to utilize the file with the most recent date")
chem_table <- read.csv("input/curated chems/substance_table_YYYY-MM-DD.csv", encoding = 'UTF-8', colClasses = "character")

message("Temporarily adding substances without DTXRID values (eventually formally curated)")

need_curate <- read_csv("input/curated chems/nonchemical_record_QA_021621.csv", col_types = cols()) %>%
  filter(Chemical == 1) %>%
  select(reported_chemical_name, reported_casrn, external_id = name) %>%
  distinct()

# Filling in missing
need_curate[names(chem_table)[!names(chem_table) %in% names(need_curate)]] = NA

# Mutate and fill in fields for substances table
sub_data <- chem_table %>%
  rbind(need_curate) %>%
  as.data.frame() %>%
  mutate(dtxsid = DSSTox_Substance_Id,
         dtxrid = DSSTox_Source_Record_Id,
         casrn = Substance_CASRN,
         preferred_name = Substance_Name,
         substance_type = Substance_Type,
         structure_inchikey = Structure_InChIKey,
         structure_formula = Structure_Formula,
         howcurated = "Standard Curation and Provided RID",
         source_sourcename = gsub('-.*', '', external_id)) %>%
  mutate(across("reported_chemical_name", ~trimws(tolower(.)))) %>%
  select(reported_casrn, reported_chemical_name, dtxsid, dtxrid, preferred_name, 
         casrn, howcurated, source_sourcename, external_id,
         substance_type, structure_inchikey, structure_formula) %>%
  distinct() %>%
  tibble::rowid_to_column(var = "substance_id") %>% # Add substance id
  left_join(source, by = c("source_sourcename" = "source_name_abbr")) %>% # Match source_sourcename to source table
  select(-source_sourcename)

sub_data$howcurated[is.na(sub_data$DTXRID)] = "Not yet curated"
sub_data$reported_casrn[sub_data$reported_casrn %in% c("", " ", "NULL")] <- NA
sub_data$reported_chemical_name[sub_data$reported_chemical_name %in% c("", " ")] <- NA 

# Fix erroneous conversion to date format --> Rearranges to correct format
if(length(sub_data$reported_casrn[grepl("/", sub_data$reported_casrn)])){
  sub_data$reported_casrn[grepl("/", sub_data$reported_casrn)] <- sapply(strsplit(sub_data$reported_casrn[grepl("/", sub_data$reported_casrn)], "/"), function(x){
    if(as.numeric(x[1]) < 10){ x[1] <- paste0(0, x[1]) }
    return(paste(x[3], x[1], x[2], sep="-"))
  })
}
if(length(sub_data$casrn[grepl("/", sub_data$casrn)])){
  sub_data$casrn[grepl("/", sub_data$casrn)] <- sapply(strsplit(sub_data$casrn[grepl("/", sub_data$casrn)], "/"), function(x){
    if(as.numeric(x[1]) < 10){ x[1] <- paste0(0, x[1]) }
    return(paste(x[3], x[1], x[2],sep = "-"))
  })
}

# Quick fix to special character cases identified
sub_data$reported_chemical_name[sub_data$reported_chemical_name == "benzo(j)fluoranthčne"] = "benzo(j)fluoranthene"
sub_data$reported_chemical_name[sub_data$reported_chemical_name == "m-xylčne/p-xylčne (somme)"] = "m-xylene/p-xylene (somme)"
sub_data$reported_chemical_name[sub_data$reported_chemical_name == "somme(b,j,k)-benzofluoranthčnes"] = "somme(b,j,k)-benzofluoranthenes"
sub_data$reported_chemical_name[sub_data$reported_chemical_name == "xylčnes totaux"] = "xylenes totaux"

if(!is.na(tryCatch({ # Try to create temp table to write to
  dbWriteTable(con, name = 'temp_table3', value = sub_data, row.names = F, overwrite = T) # Creating temp data table
},
error = function(cond){ message("Error message: ", cond); return(NA) },
warning = function(cond){ message("Warning message: ", cond); return(NA) })
)
){

  if (dbExistsTable(con, "substances")) {
    sub_fields <- dbListFields(con, "substances")
    temp_info <- dbColumnInfo(con, "temp_table3") # Getting info to match datatypes
  } else { message("Substances table does not exist... Need to create it first...") }
  message("Updating substances table...")
  dbSendStatement(con, paste0("INSERT INTO substances (", toString(names(sub_data)),")
              SELECT ", toString(names(sub_data))," FROM temp_table3"))
  if (length(dbListResults(con))) {dbClearResult(dbListResults(con)[[1]])}; dbDisconnect(con); message("Done...")
  message("Temporarily added substances without DTXRID values (eventually formally curated)")
} else {
  message("Failed to updated substances table")
}
