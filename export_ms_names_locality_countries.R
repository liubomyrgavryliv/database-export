library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/post-doc/R scripts/export_to_SQL/")
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
countries_iso <- tbl(conn, 'countries_list')

# PROCESS DATA -----------------------------------------------------------------
ms_names_locality_countries <- initial %>%
  select(Mineral_Name, Country) %>%
  filter(!is.na(Country)) %>%
  mutate(Country = str_split(Country, ';')) %>%
  unchop(Country, keep_empty = TRUE) %>%
  left_join(countries_iso, by = c('Country' = 'name'), copy=TRUE) %>%
  select(Mineral_Name, id) %>%
  left_join(ms_species, by=c('Mineral_Name'='name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # COMPARE MINERALS WITH MASTER TABLE !
  select(mineral_id, id) %>%
  rename(country_id=id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_names_locality_countries;")
dbWriteTable(conn, "ms_names_locality_countries", ms_names_locality_countries, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_names_locality_countries ------------------------------------------------------------
write_csv(ms_names_locality_countries, path = paste0(path, 'ms_names_locality_countries.csv'), na='')
