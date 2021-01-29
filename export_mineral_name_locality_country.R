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


country_list <- tbl(conn, 'country_list')
mineral_name_locality <- dbSendQuery(conn, "SELECT ml.mineral_name, mnl.id FROM 
                                            mineral_name_locality mnl INNER JOIN mineral_list ml on ml.mineral_id = mnl.mineral_id;") %>%
  dbFetch() %>%
  arrange(mineral_name)

# PROCESS DATA -----------------------------------------------------------------
mineral_name_locality_country <- initial %>%
  select(Mineral_Name, Country) %>%
  filter(!is.na(Country)) %>%
  mutate(Country = str_split(Country, ';')) %>%
  unchop(Country, keep_empty = TRUE) %>%
  left_join(country_list, by = c('Country' = 'country_name'), copy=TRUE) %>%
  select(Mineral_Name, country_id) %>%
  left_join(mineral_name_locality, by=c('Mineral_Name'='mineral_name'), copy=TRUE) %>%
  filter(!is.na(id)) %>% # COMPARE MINERALS WITH MASTER TABLE !
  rename(name_locality_id=id) %>%
  select(name_locality_id, country_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_name_locality_country RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_name_locality_country", mineral_name_locality_country, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
