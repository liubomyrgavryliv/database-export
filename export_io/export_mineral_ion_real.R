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


mineral_list = tbl(conn, 'mineral_list')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:N',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse ions ----------------------------------------------------------------
mineral_ion_real <- initial %>%
  select(Mineral_Name, anions_real, cations_real, silicates_real, other_real) %>%
  rename(anion=anions_real, cation=cations_real, silicate=silicates_real, other=other_real) %>%
  filter(!is.na(anion) | !is.na(cation) | !is.na(silicate) | !is.na(other)) %>%
  left_join(mineral_list, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  select(mineral_id, anion, cation, silicate, other)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_ion_real RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_ion_real", mineral_ion_real, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
