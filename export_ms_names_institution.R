library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
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
  range = 'A:AK',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

country_list <- tbl(conn, 'country_list')
mineral_list = tbl(conn, 'mineral_list')
# PROCESS DATA -----------------------------------------------------------------
mineral_name_institution <- initial %>%
  filter(!is.na(`GROUP/INSTITUTION`)) %>%
  select(Mineral_Name, `GROUP/INSTITUTION`, About, Nationality__1)

# COMPARE WITH languages_list --------- OMIT THIS STEP !!!!! -------------------
countries_bugs <- ms_names_institution %>%
  anti_join(country_list, by = c('Nationality__1' = 'country_name'), copy=TRUE)

# replace countries names with ids
mineral_name_institution <-
  mineral_name_institution %>%
  left_join(country_list, by=c('Nationality__1' = 'country_name'), copy=TRUE) %>%
  select(Mineral_Name, `GROUP/INSTITUTION`, About, country_id) %>%
  rename(name=Mineral_Name, institution_name = `GROUP/INSTITUTION`, note=About) %>%
  left_join(mineral_list, by=c('name'='mineral_name'), copy=TRUE) %>%
  rename(note=note.x) %>%
  select(mineral_id, institution_name, note, country_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_name_institution RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_name_institution", mineral_name_institution, append=TRUE)


dbDisconnect(conn)
