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


ion_list = tbl(conn, 'ion_list')
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

# CHECK IONS FROM NS ACCORDING TO ION_LIST DB TABLE
anions <- initial %>%
  select(Mineral_Name, anions_theoretical) %>%
  mutate(ion = str_split(anions_theoretical, ';')) %>%
  unchop(ion, keep_empty = TRUE) %>% #parse ion level
  distinct(ion) %>%
  arrange(ion)

ion_list_anions <- ion_list %>%
  filter(ion_type_id == 1)

check_anions <- anions %>%
  left_join(ion_list_anions, by=c('ion'='formula'),copy=TRUE) %>%
  filter(is.na(ion_id))

cations <- initial %>%
  select(Mineral_Name, cations_theoretical) %>%
  mutate(ion = str_split(cations_theoretical, ';')) %>%
  unchop(ion, keep_empty = TRUE) %>% #parse ion level
  distinct(ion) %>%
  arrange(ion)

ion_list_cations <- ion_list %>%
  filter(ion_type_id == 2)

check_cations <- cations %>%
  left_join(ion_list_cations, by=c('ion'='formula'),copy=TRUE) %>%
  filter(is.na(ion_id))

silicates <- initial %>%
  select(Mineral_Name, silicates_theoretical) %>%
  mutate(ion = str_split(silicates_theoretical, ';')) %>%
  unchop(ion, keep_empty = TRUE) %>% #parse ion level
  distinct(ion) %>%
  arrange(ion)

ion_list_silicates <- ion_list %>%
  filter(ion_type_id == 3)

check_silicates <- silicates %>%
  left_join(ion_list, by=c('ion'='formula'),copy=TRUE) %>%
  filter(is.na(ion_id))

neutral <- initial %>%
  select(Mineral_Name, other_theoretical) %>%
  mutate(ion = str_split(other_theoretical, ';')) %>%
  unchop(ion, keep_empty = TRUE) %>% #parse ion level
  distinct(ion) %>%
  arrange(ion)

ion_list_neutral <- ion_list %>%
  filter(ion_type_id == 4)

check_neutral <- neutral %>%
  left_join(ion_list, by=c('ion'='formula'),copy=TRUE) %>%
  filter(is.na(ion_id))

# FINISH CHECK

# Create subsets for ions
anions <- initial %>%
  select(Mineral_Name, anions_theoretical) %>%
  mutate(formula = str_split(anions_theoretical, ';')) %>%
  unchop(formula, keep_empty = TRUE) %>%
  left_join(ion_list_anions, by='formula', copy=TRUE) %>%
  left_join(mineral_list, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  filter(!is.na(ion_id)) %>%
  filter(!is.na(mineral_id)) %>%
  select(mineral_id, ion_id)

cations <- initial %>%
  select(Mineral_Name, cations_theoretical) %>%
  mutate(formula = str_split(cations_theoretical, ';')) %>%
  unchop(formula, keep_empty = TRUE) %>%
  left_join(ion_list_cations, by='formula', copy=TRUE) %>%
  left_join(mineral_list, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  filter(!is.na(ion_id)) %>%
  filter(!is.na(mineral_id)) %>%
  select(mineral_id, ion_id)

silicates <- initial %>%
  select(Mineral_Name, silicates_theoretical) %>%
  mutate(formula = str_split(silicates_theoretical, ';')) %>%
  unchop(formula, keep_empty = TRUE) %>%
  left_join(ion_list_silicates, by='formula', copy=TRUE) %>%
  left_join(mineral_list, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  filter(!is.na(ion_id)) %>%
  filter(!is.na(mineral_id)) %>%
  select(mineral_id, ion_id)

other <- initial %>%
  select(Mineral_Name, other_theoretical) %>%
  mutate(formula = str_split(other_theoretical, ';')) %>%
  unchop(formula, keep_empty = TRUE) %>%
  left_join(ion_list_neutral, by='formula', copy=TRUE) %>%
  left_join(mineral_list, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  filter(!is.na(ion_id)) %>%
  filter(!is.na(mineral_id)) %>%
  select(mineral_id, ion_id)

mineral_ion_theoretical <- union(anions,cations,silicates,other) %>%
  arrange(mineral_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_ion_theoretical RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_ion_theoretical", mineral_ion_theoretical, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
