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

anions <- initial %>%
  select(anion_subunites) %>%
  mutate(anion_subunites=str_split(anion_subunites, ';')) %>%
  unchop(anion_subunites, keep_empty = TRUE) %>%
  distinct(anion_subunites) %>%
  filter(!is.na(anion_subunites)) %>%
  left_join(ion_list_anions, by=c('anion_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

ion_list_cations <- ion_list %>%
  filter(ion_type_id == 2)

cations <- initial %>%
  select(cation_subunites) %>%
  mutate(cation_subunites=str_split(cation_subunites, ';')) %>%
  unchop(cation_subunites, keep_empty = TRUE) %>%
  distinct(cation_subunites) %>%
  filter(!is.na(cation_subunites)) %>%
  left_join(ion_list_cations, by=c('cation_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

ion_list_silicates <- ion_list %>%
  filter(ion_type_id == 3)

silicates <- initial %>%
  select(silicate_subunites) %>%
  mutate(silicate_subunites=str_split(silicate_subunites, ';')) %>%
  unchop(silicate_subunites, keep_empty = TRUE) %>%
  distinct(silicate_subunites) %>%
  filter(!is.na(silicate_subunites)) %>%
  left_join(ion_list_silicates, by=c('silicate_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

ion_list_other <- ion_list %>%
  filter(ion_type_id == 4)

other <- initial %>%
  select(other_subunites) %>%
  mutate(other_subunites=str_split(other_subunites, ';')) %>%
  unchop(other_subunites, keep_empty = TRUE) %>%
  distinct(other_subunites) %>%
  filter(!is.na(other_subunites)) %>%
  left_join(ion_list_other, by=c('other_subunites'='formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

# FINISH Check -------------------------------------------------------------
# Create subsets and merge into ion_subunit

anions <- initial %>%
  select(formula, anion_subunites) %>%
  mutate(anion_subunites=str_split(anion_subunites, ';')) %>%
  unchop(anion_subunites, keep_empty = TRUE) %>%
  filter(!is.na(anion_subunites)) %>%
  left_join(ion_list_anions, by=c('anion_subunites'='formula'), copy=TRUE) %>%
  select(formula, ion_id) %>%
  rename(subunit_id=ion_id) %>%
  left_join(ion_list, by=c('formula'='formula'), copy=TRUE) %>%
  select(ion_id, subunit_id)

cations <- initial %>%
  select(formula, cation_subunites) %>%
  mutate(cation_subunites=str_split(cation_subunites, ';')) %>%
  unchop(cation_subunites, keep_empty = TRUE) %>%
  filter(!is.na(cation_subunites)) %>%
  left_join(ion_list_cations, by=c('cation_subunites'='formula'), copy=TRUE) %>%
  select(formula, ion_id) %>%
  rename(subunit_id=ion_id) %>%
  left_join(ion_list, by=c('formula'='formula'), copy=TRUE) %>%
  select(ion_id, subunit_id)

silicates <- initial %>%
  select(formula, silicate_subunites) %>%
  mutate(silicate_subunites=str_split(silicate_subunites, ';')) %>%
  unchop(silicate_subunites, keep_empty = TRUE) %>%
  filter(!is.na(silicate_subunites)) %>%
  left_join(ion_list_silicates, by=c('silicate_subunites'='formula'), copy=TRUE) %>%
  select(formula, ion_id) %>%
  rename(subunit_id=ion_id) %>%
  left_join(ion_list, by=c('formula'='formula'), copy=TRUE) %>%
  select(ion_id, subunit_id)

other <- initial %>%
  select(formula, other_subunites) %>%
  mutate(other_subunites=str_split(other_subunites, ';')) %>%
  unchop(other_subunites, keep_empty = TRUE) %>%
  filter(!is.na(other_subunites)) %>%
  left_join(ion_list_other, by=c('other_subunites'='formula'), copy=TRUE) %>%
  select(formula, ion_id) %>%
  rename(subunit_id=ion_id) %>%
  left_join(ion_list, by=c('formula'='formula'), copy=TRUE) %>%
  select(ion_id, subunit_id)

ion_subunit <- union(anions, cations, silicates, other) %>%
  arrange(ion_id, subunit_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE ion_subunit RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "ion_subunit", ion_subunit, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
