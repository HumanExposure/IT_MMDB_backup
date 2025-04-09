## Script to check and push a CASRN formatting fix to harmonized tables (doesn't affect raw data)
## Authors: Jonathan Taylor Wall & Rachel Hencher
## Created: Unknown
## Last updated: 2024-08-13

print("Running script for CASRN fix...")
required.packages = c('plyr', 'dplyr', 'magrittr')
invisible(lapply(required.packages, require, character.only = TRUE, warn.conflicts = FALSE))

# Connect to the database
connect_to_mmdb <- dbConnect(PostgreSQL(),
                             user = Sys.getenv("postgres_user"),
                             password = Sys.getenv("postgres_pass"),
                             host = Sys.getenv("postgres_host"),
                             dbname = Sys.getenv("postgres_dbname"))

con <- connect_to_mmdb
dbGetQuery(con, "set search_path to mmdb")

for(harm_tbl in c("harmonized_aggregate", "harmonized_raw")){
  message("Pulling data for ", harm_tbl, " at: ", Sys.time())
  dat <- tbl(con, harm_tbl) %>% 
    select(source_id, file_id, record_id, reported_casrn) %>% 
    filter(reported_casrn %like% "%/%/%") %>% # Find CASRN with date '/' marks
    collect()
  dat$fix <- dat$reported_casrn
  # Fix erroneous conversion to date format --> rearrange to correct format
  if(length(dat$fix[grepl("/", dat$reported_casrn)])){
    dat$fix[grepl("/", dat$reported_casrn)] <- sapply(strsplit(dat$reported_casrn[grepl("/", 
                                                                                        dat$reported_casrn)], "/"), 
                                                      function(x){
                                                        if(as.numeric(x[1]) < 10){ x[1] <- paste0(0, x[1]) 
                                                        }
                                                        return(paste(x[3], x[1], x[2], sep = "-"))
                                                      })
  }
  print(paste("...Writing fixed casrn values to temp_table: ", Sys.time())) # Push mapped variable back to database
  dbWriteTable(con, name = 'temp_table', value = dat, row.names = F, overwrite = T)
  message("...Writing temp_table to ", harm_tbl, " at: ", Sys.time())
  # Sent update statements matching to file ID and record ID
  dbSendStatement(con,
  paste0("UPDATE ", harm_tbl, " SET reported_casrn = temp_table.fix FROM temp_table WHERE ", harm_tbl, ".file_id = temp_table.file_id AND ", harm_tbl, ".record_id = temp_table.record_id")
  )
  message("Done with ", harm_tbl, " at: ", Sys.time())
}
