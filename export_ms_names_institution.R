library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
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
  range = 'A:AK',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

countries_list <- tbl(conn, 'countries_list')
ms_species = tbl(conn, 'ms_species')
# PROCESS DATA -----------------------------------------------------------------
ms_names_institution <- initial %>%
  filter(!is.na(`GROUP/INSTITUTION`)) %>%
  select(Mineral_Name, `GROUP/INSTITUTION`, About, Nationality__1)

# COMPARE WITH languages_list --------- OMIT THIS STEP !!!!! -------------------
countries_bugs <- ms_names_institution %>%
  anti_join(countries_list, by = c('Nationality__1' = 'name'), copy=TRUE)

# replace countries names with ids
ms_names_institution <-
  ms_names_institution %>%
  left_join(countries_list, by=c('Nationality__1' = 'name'), copy=TRUE) %>%
  select(Mineral_Name, `GROUP/INSTITUTION`, About, id) %>%
  rename(name=Mineral_Name, institution_name = `GROUP/INSTITUTION`, note=About, country_id=id) %>%
  left_join(ms_species, by=c('name'='mineral_name'), copy=TRUE) %>%
  rename(note=note.x) %>%
  select(mineral_id, institution_name, note, country_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_names_institution;")
dbWriteTable(conn, "ms_names_institution", ms_names_institution, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(ms_names_institution, path = paste0(path, 'ms_names_institution.csv'), na='')
