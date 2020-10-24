library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  range = 'A:AV',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

ms_species <- tbl(conn, 'ms_species')
localities_types <- tbl(conn, 'locality_type_list')

# PROCESS DATA -----------------------------------------------------------------
ms_names_locality <- initial %>%
  select(Mineral_Name, LOCALITY, Type, Note) %>%
  filter(!is.na(LOCALITY)) %>%
  left_join(localities_types, by = c('Type' = 'locality_type'), copy = TRUE) %>%
  mutate(locality_type = id) %>%
  select(-c(id, note)) %>%
  left_join(ms_species, by=c('Mineral_Name'='name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # COMPARE MINERALS WITH MASTER TABLE !
  select(mineral_id, LOCALITY, locality_type, Note) %>%
  rename(locality_name=LOCALITY, note=Note)

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_names_locality;")
dbWriteTable(conn, "ms_names_locality", ms_names_locality, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_names_localities ------------------------------------------------------------
write_csv(ms_names_locality, path = paste0(path, 'ms_names_locality.csv'), na='')
