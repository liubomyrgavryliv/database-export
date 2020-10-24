library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/post-doc/R scripts/export_to_SQL/")
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

# LOAD DATA -------------------------------------------------------------------
ms_species <- tbl(conn, 'ms_species')
nationalities_list <- tbl(conn, 'nationalities_list')

# CREATE DATA TO PROCESS FOR VITALII - OMIT THIS STEP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# FIND named by >1 person
ms_names_person <- initial %>%
  select(Mineral_Name, Person, Born, Died, Role, Gender, Nationality) %>%
  rename(nationality = Nationality) %>%
  filter(!is.na(nationality)) %>%
  #filter(str_detect(Person, ';')) %>%
  mutate(Person = str_split(Person,'; ?'),
         Born = str_split(Born,'; ?'),
         Died = str_split(Died,'; ?'),
         Role = str_split(Role,'; ?'),
         Gender = str_split(Gender,'; ?'),
         nationality = str_split(nationality,'; ?')) %>%
  unchop(cols = c('Person', 'Born', 'Died', 'Role', 'Gender', 'nationality'), keep_empty = TRUE) %>%
  left_join(nationalities_list,by=c('nationality' = 'nationality_name'), copy=TRUE) %>%
  select(Mineral_Name, Person, Born, Died, Role, Gender, id) %>%
  left_join(ms_species, by=c('Mineral_Name'='name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # COMPARE MINERALS WITH MASTER TABLE !
  select(mineral_id, Person, Born, Died, Role, Gender, id) %>%
  rename(person=Person, born=Born, died=Died,role=Role,gender=Gender,nationality=id)


# PROCESS DATA ----- THIS IS NOT NEEDED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ms_names_person <- initial %>%
  select(Mineral_Name, Nationality) %>%
  rename(nationality = Nationality) %>%
  filter(!is.na(nationality)) %>%
  mutate(nationality = str_replace_all(nationality, '-', ';')) %>%
  mutate(nationality = str_split(nationality, ';')) %>%
  unchop(nationality, keep_empty = TRUE) %>%
  mutate(nationality = str_replace_all(nationality, '^\\s+', ''))

ms_names_person_two <- initial %>%
  select(Mineral_Name, Person, Nationality) %>%
  rename(nationality = Nationality) %>%
  filter(!is.na(nationality)) %>%
  filter(str_detect(nationality, ';|-') & str_detect(Person, ';', negate=TRUE))
# CHECK-------------------------------------------------------------------------
check <- ms_names_person %>% 
  select(nationality) %>%
  distinct(nationality, .keep_all = TRUE) %>%
  arrange(nationality)

nationalities_review <- check %>%
  anti_join(nationalities_list, by=c('nationality' = 'nationality_name'), copy=TRUE)

# disconnect from DB
dbSendQuery(conn, "DELETE FROM ms_names_person;")
dbWriteTable(conn, "ms_names_person", ms_names_person, append=TRUE)
dbDisconnect(conn)

# EXPORT DATA ------------------------------------------------------------------
write_csv(ms_names_person, path = paste0(path, 'ms_names_person.csv'), na='')
