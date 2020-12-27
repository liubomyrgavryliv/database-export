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

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Ions_data',
  range = 'A:X',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse ions ----------------------------------------------------------------

# CHECK SUBUNITS
ion_list_anions <- ion_list %>%
  filter(ion_type_id == 1)

ion_check <- initial %>%
  select(anion_subunites) %>%
  mutate(anion_subunites=str_split(anion_subunites, ';')) %>%
  unchop(anion_subunites, keep_empty = TRUE) %>%
  distinct(anion_subunites) %>%
  filter(!is.na(anion_subunites)) %>%
  left_join(ion_list_anions, by=c('anion_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

ion_list_anions <- ion_list %>%
  filter(ion_type_id == 2)

ion_check <- initial %>%
  select(anion_subunites) %>%
  mutate(anion_subunites=str_split(anion_subunites, ';')) %>%
  unchop(anion_subunites, keep_empty = TRUE) %>%
  distinct(anion_subunites) %>%
  filter(!is.na(anion_subunites)) %>%
  left_join(ion_list_anions, by=c('anion_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

ion_list_anions <- ion_list %>%
  filter(ion_type_id == 3)

ion_check <- initial %>%
  select(anion_subunites) %>%
  mutate(anion_subunites=str_split(anion_subunites, ';')) %>%
  unchop(anion_subunites, keep_empty = TRUE) %>%
  distinct(anion_subunites) %>%
  filter(!is.na(anion_subunites)) %>%
  left_join(ion_list_anions, by=c('anion_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

ion_list_anions <- ion_list %>%
  filter(ion_type_id == 4)

ion_check <- initial %>%
  select(anion_subunites) %>%
  mutate(anion_subunites=str_split(anion_subunites, ';')) %>%
  unchop(anion_subunites, keep_empty = TRUE) %>%
  distinct(anion_subunites) %>%
  filter(!is.na(anion_subunites)) %>%
  left_join(ion_list_anions, by=c('anion_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))
initial_ions <- initial %>%
  select(formula) %>%
  mutate(ion_id=row_number())

ion_list <- initial %>%
  select(ion_type_name, ion_name, formula, formula_with_oxidation, overall_charge, variety_of, expressed_as, element_or_sulfide,
         ion_class_name, ion_subclass_name, ion_group_name, ion_subgroup_name, structure_description, geometry) %>%
  left_join(ion_type_list, by='ion_type_name', copy=TRUE) %>%
  left_join(ion_class_list, by='ion_class_name', copy=TRUE) %>%
  left_join(ion_subclass_list, by='ion_subclass_name', copy=TRUE) %>%
  left_join(ion_group_list, by='ion_group_name', copy=TRUE) %>%
  left_join(ion_subgroup_list, by='ion_subgroup_name', copy=TRUE) %>%
  left_join(initial_ions, by=c('variety_of'='formula'), copy=TRUE) %>%
  select(ion_type_id, ion_name, formula, formula_with_oxidation, overall_charge, ion_id, expressed_as, element_or_sulfide, ion_class_id, ion_subclass_id,
         ion_group_id, ion_subgroup_id, structure_description, geometry) %>%
  rename(variety_of = ion_id)


# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE ion_list RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "ion_list", ion_list, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
