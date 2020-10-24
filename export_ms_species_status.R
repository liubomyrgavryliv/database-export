library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/post-doc/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'

# LOAD data ---------------------------------------------------------------------
mn_status <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:Z',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, all_indexes)

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

ms_species <- tbl(conn, 'ms_species')
ms_species_status_db <- tbl(conn, 'ms_species_status')
# PROCESS data 

ms_species_status <- mn_status %>%
    mutate(all_indexes = str_split(all_indexes, '; ')) %>%
    unchop(all_indexes, keep_empty = TRUE) %>%
    left_join(ms_species, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>% # CHECK IF PRESENT IN MASTER TABLE
    filter(!is.na(mineral_id)) %>%
    select(mineral_id, all_indexes) %>%
    rename('status_id' = 'all_indexes')

# ADD THE MISSING MINERALS TO DB
ms_species_status_new <- ms_species_status %>%
  anti_join(ms_species_status_db, by=c('mineral_id'), copy=TRUE)
  
# UPLOAD DATA TO DB
# dbSendQuery(conn, "DELETE FROM ms_species_status;")
dbWriteTable(conn, "ms_species_status", ms_species_status_new, append=TRUE)
dbDisconnect(conn)

# EXPORT data
write_csv(ms_species_status, path = paste0(path, 'ms_species_status.csv'), na='')
