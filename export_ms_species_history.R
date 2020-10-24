library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'


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

ms_species <- tbl(conn, 'ms_species')

# PROCESS DATA -----------------------------------------------------------------
ms_species_history <- initial %>%
  select(Mineral_Name, discovery_year_min, discovery_year_max, discovery_year_note, First_usage_date, `First known use`) %>%
  rename(first_usage_date = First_usage_date, first_known_use = `First known use`) %>%
  mutate(discovery_year_min = ifelse(str_detect(discovery_year_min, '[Uu]nknown'), NA, discovery_year_min),
         discovery_year_max = ifelse(str_detect(discovery_year_max, '[Uu]nknown'), NA, discovery_year_max),
         first_usage_date = ifelse(str_detect(first_usage_date, '[Uu]nknown'), NA, first_usage_date)) %>%
  filter(!is.na(discovery_year_min) | !is.na(discovery_year_max) | !is.na(discovery_year_note) | !is.na(first_usage_date) | 
           !is.na(first_known_use)) %>%
  left_join(ms_species, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # COMPARE IF ALL ARE PRESENT IN MASTER TABLE !
  select(mineral_id, discovery_year_min, discovery_year_max, discovery_year_note, first_usage_date, first_known_use)


# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_species_history;")
dbWriteTable(conn, "ms_species_history", ms_species_history, append=TRUE)
dbDisconnect(conn)

# EXPORT DATA ------------------------------------------------------------------
write_csv(ms_species_history, path = paste0(path, 'ms_species_history.csv'), na='')
