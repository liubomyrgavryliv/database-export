library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  #range = 'A:AY',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

ms_species = tbl(conn, 'ms_species')
# LOAD DATA -------------------------------------------------------------------
ms_names_chemistry <- initial %>%
  select(Mineral_Name, CHEMISTRY) %>%
  filter(!is.na(CHEMISTRY))

# omit this after adding minerals by VITALII !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ms_names_chemistry <- ms_names_chemistry %>%
  inner_join(initial, by=c('Named Relation' = 'Mineral_Name')) %>%
  mutate(`Named Relation` = ifelse(is.na()))
  select(Mineral_Name, CHEMISTRY, `Elements/Ions`, `Named Relation`)
  
# CHECK NAMED RELATION FOR VITALII - OMIT THIS STEP!!!!!!!!!!!!!!!!!!!!!!!!!!!!
unique_ions <- ms_names_chemistry %>%
  select(`Elements/Ions`) %>%
  filter(!is.na(`Elements/Ions`)) %>%
  distinct() %>%
  arrange()

related_names <- ms_names_chemistry %>%
  select(`Named Relation`) %>%
  filter(!is.na(`Named Relation`)) %>%
  distinct() %>%
  arrange()

related_names_bugs <- related_names %>%
  anti_join(initial, by=c('Named Relation' = 'Mineral_Name'))

ms_names_chemistry <- ms_names_chemistry %>%
  filter(!`Named Relation` %in% related_names_bugs$`Named Relation`) %>%
  rename(name = Mineral_Name, relation = 'Named Relation', note = CHEMISTRY) %>%
  select(name, relation, note) %>%
  distinct(name, .keep_all = TRUE)

# Find duplicates
check <- ms_names_chemistry %>%
  group_by(Mineral_Name) %>%
  filter(n()>1)

ms_names_chemistry <- ms_names_chemistry %>%
  left_join(ms_species, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  select(mineral_id, CHEMISTRY, `Named Relation`) %>%
  left_join(ms_species, by=c(`Named Relation` = 'mineral_name'), copy=TRUE) %>%
  select(mineral_id.x, CHEMISTRY, mineral_id.y) %>%
  rename(mineral_id=mineral_id.x,relation_id=mineral_id.y,note=CHEMISTRY)
# UPLOAD DATA TO DB ------------------------------------------------------------
dbSendQuery(conn, "DELETE FROM ms_names_chemistry;")
dbWriteTable(conn, "ms_names_chemistry", ms_names_chemistry, append=TRUE)
dbDisconnect(conn)

# EXPORT DATA ------------------------------------------------------------------
write_csv(ms_names_chemistry, path = paste0(path, 'ms_names_chemistry.csv'), na='')

# disconnect from DB
dbDisconnect(conn)

