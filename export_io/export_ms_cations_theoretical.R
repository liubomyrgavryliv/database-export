library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')


ms_species = tbl(conn, 'ms_species')
#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:I',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

io_cations <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Cations',
  range = 'A:A',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# CREATE DATA TO PROCESS FOR VITALII - OMIT THIS STEP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# create cations_theoretical and export to masterlist
ms_cations_theoretical <- 
  initial %>%
  select(Mineral_Name, cations) %>%
  mutate(cations_theoretical = str_split(cations, ';')) %>%
  unchop(cations_theoretical, keep_empty = TRUE) %>%
  mutate(cations_theoretical = str_replace(cations_theoretical, ' x.*', '')) %>%
  mutate(cations_theoretical = str_replace(cations_theoretical, '^\\((?!.*\\)[^\\=].)', '')) %>%
  mutate(cations_theoretical = str_replace(cations_theoretical, '^\\((?=.*\\)$)', '')) %>%
  mutate(cations_theoretical = str_replace(cations_theoretical, '\\=.*', '')) %>%
  mutate(cations_theoretical = str_replace(cations_theoretical, '\\)$', '')) %>%
  mutate(cations_theoretical = str_split(cations_theoretical, ' \\+ ')) %>%
  unchop(cations_theoretical, keep_empty = TRUE) %>%
  group_by(Mineral_Name) %>%
  summarise(cations_theoretical = paste(unique(cations_theoretical), collapse=';')) %>%
  mutate(cations_theoretical = na_if(cations_theoretical, "NA"))

# create sorted df for export into masterlist
export <- initial %>%
  select(Mineral_Name) %>%
  left_join(ms_cations_theoretical, by='Mineral_Name')

# cross-check with ions_database
cations_unique <- 
  initial %>%
  select(cations_theoretical) %>%
  mutate(cations_theoretical = str_split(cations_theoretical, ';')) %>%
  unchop(cations_theoretical, keep_empty = TRUE) %>%
  distinct(cations_theoretical) %>%
  arrange(cations_theoretical)

io_absent <- io_cations %>%
  anti_join(cations_unique, by = c('Ion'='cations_theoretical'))

ns_absent <- cations_unique %>%
  anti_join(io_cations, by = c('cations_theoretical' = 'Ion'))

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM el_data;")
dbWriteTable(conn, "el_data", el_data, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(export, path = paste0(path, 'ms_cations_theoretical.csv'), na='')
write_csv(io_absent, path = paste0(path, 'cations_absent_in_ns.csv'), na='')
write_csv(ns_absent, path = paste0(path, 'cations_absent_in_ions_db.csv'), na='')
