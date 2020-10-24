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

names_other_list <- tbl(conn, 'names_other_list')
ms_species <- tbl(conn, 'ms_species')
# LOAD DATA -------------------------------------------------------------------
ms_names_other <- initial %>%
  select(Mineral_Name, CHEMISTRY, other, note) %>%
  mutate(other = ifelse(is.na(CHEMISTRY), other, paste0('Chemistry',';', other))) %>%
  mutate(other = str_replace(other, ';NA', '')) %>%
  unite(note, c('CHEMISTRY', 'note'), sep=';',remove=TRUE,na.rm=TRUE) %>%
  filter(!is.na(other)) %>%
  mutate(other = str_split(other, ';'),
         note = str_split(note, ';')) %>%
  unchop(cols=c('other','note'), keep_empty = T) %>%
  left_join(names_other_list, by=c('other' = 'type'), copy=TRUE) %>%
  select(Mineral_Name, id, note.x) %>%
  rename(mineral_name=Mineral_Name, type=id,note=note.x) %>%
  left_join(ms_species, by='mineral_name', copy=TRUE) %>%
  select(mineral_id, type, note.x) %>%
  rename(note=note.x)

  
# UPLOAD DATA TO DB ------------------------------------------------------------
dbSendQuery(conn, "DELETE FROM ms_names_other;")
dbWriteTable(conn, "ms_names_other", ms_names_other, append=TRUE)
dbDisconnect(conn)

# EXPORT DATA ------------------------------------------------------------------
write_csv(ms_names_other, path = paste0(path, 'ms_names_other.csv'), na='')

# disconnect from DB
dbDisconnect(conn)
