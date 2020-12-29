library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
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

mineral_list <- tbl(conn, 'mineral_list')
locality_type_list <- tbl(conn, 'locality_type_list')

# PROCESS DATA -----------------------------------------------------------------
mineral_name_locality <- initial %>%
  select(Mineral_Name, LOCALITY, Type, Note) %>%
  filter(!is.na(LOCALITY)) %>%
  left_join(locality_type_list, by = c('Type' = 'locality_type_name'), copy = TRUE) %>%
  select(-c( note)) %>%
  left_join(mineral_list, by=c('Mineral_Name'='mineral_name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # COMPARE MINERALS WITH MASTER TABLE !
  select(mineral_id, LOCALITY, locality_type_id, Note) %>%
  rename(locality_name=LOCALITY, note=Note)


# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_name_locality RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_name_locality", mineral_name_locality, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
