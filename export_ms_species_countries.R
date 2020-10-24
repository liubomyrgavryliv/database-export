library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'

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

countries_iso <- tbl(conn, 'countries_list')
ms_species <- tbl(conn, 'ms_species')

# PROCESS DATA -----------------------------------------------------------------
ms_species_countries <- initial %>%
  select(Mineral_Name, discovery_country, discovery_country_note) %>%
  rename(name = Mineral_Name) %>%
  mutate(discovery_country = ifelse(str_detect(discovery_country, '[Uu]nknown'), NA, discovery_country)) %>%
  mutate(discovery_country = str_replace_all(discovery_country, ' / ', '/')) %>%
  mutate(discovery_country = str_split(discovery_country, '/')) %>%
  unchop(discovery_country, keep_empty = TRUE) %>%
  mutate(discovery_country = ifelse(is.na(discovery_country) & !is.na(discovery_country_note), 'unknown', discovery_country)) %>%
  filter(!is.na(discovery_country)) %>%
  left_join(countries_iso, by = c('discovery_country' = 'name'), copy = TRUE) %>%
  select(name, country_id, discovery_country_note) %>%
  left_join(ms_species, by=c('name' = 'mineral_name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # COMPARE WITH MINERALS IN MASTER TABLE !
  select(mineral_id, country_id, discovery_country_note) %>%
  rename(note=discovery_country_note)

# GET UNIQUE COUNTRIES FOR SCREENING - OMIT THIS STEP --------------------------
countries <- ms_species_countries %>%
  distinct(discovery_country) %>%
  filter(!is.na(discovery_country)) %>%
  arrange(discovery_country)

# COMPARE WITH countries_list 
countries_bugs <- countries %>%
  anti_join(countries_iso, by = c('discovery_country' = 'name'), copy=TRUE)

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_species_countries;")
dbWriteTable(conn, "ms_species_countries", ms_species_countries, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(ms_species_countries, path = paste0(path, 'ms_species_countries.csv'), na='')

