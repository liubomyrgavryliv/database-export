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
languages_list <- tbl(conn, 'languages_list')
# PROCESS DATA -----------------------------------------------------------------
ms_names_language <- initial %>%
  select(Mineral_Name, LANGUAGE, Meaning, Stem1, Stem2, Stem3) %>%
  filter(!is.na(LANGUAGE)) %>%
  mutate(LANGUAGE = str_split(LANGUAGE, ';')) %>%
  unchop(LANGUAGE, keep_empty = TRUE) %>%
  left_join(languages_list, by=c('LANGUAGE' = 'language_name'), copy=TRUE) %>%
  select(Mineral_Name, language_id, Meaning, Stem1, Stem2, Stem3) %>%
  left_join(ms_species, by=c('Mineral_Name'='mineral_name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>%                                  # COMPARE MINERALS WITH MASTER TABLE !
  select(mineral_id, language_id, Meaning, Stem1, Stem2, Stem3) %>%
  rename(meaning=Meaning,stem_1=Stem1,stem_2=Stem2,stem_3=Stem3)


# COMPARE WITH languages_list --------- OMIT THIS STEP !!!!! -------------------
languages_bugs <- ms_names_language %>%
  anti_join(languages_list, by = c('LANGUAGE' = 'language_name'), copy=TRUE)

# disconnect from DB
dbSendQuery(conn, "DELETE FROM ms_names_language;")
dbWriteTable(conn, "ms_names_language", ms_names_language, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(ms_names_language, path = paste0(path, 'ms_names_language.csv'), na='')

# disconnect from DB
dbDisconnect(conn)
