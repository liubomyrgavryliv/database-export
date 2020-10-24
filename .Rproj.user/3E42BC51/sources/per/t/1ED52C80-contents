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
ms_names_chemistry_ions <- initial %>%
  select(Mineral_Name, `Elements/Ions`) %>%
  filter(!is.na(`Elements/Ions`)) %>%
  mutate(`Elements/Ions` = str_split(`Elements/Ions`, ';')) %>%
  unchop(`Elements/Ions`, keep_empty = T) %>%
  rename(name = Mineral_Name, ion = `Elements/Ions`) %>%
  distinct() %>%
  left_join(ms_species, by=c('name'='mineral_name'), copy=TRUE) %>%
  select(mineral_id, ion)


# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_names_chemistry_ions;")
dbWriteTable(conn, "ms_names_chemistry_ions", ms_names_chemistry_ions, append=TRUE)
dbDisconnect(conn)

# EXPORT DATA ------------------------------------------------------------------
write_csv(ms_names_chemistry_ions, path = paste0(path, 'ms_names_chemistry_ions.csv'), na='')

