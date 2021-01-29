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
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'Groups_ver1',
  range = 'A:H',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

mineral_list <- tbl(conn, 'mineral_list')

# PROCESS DATA -----------------------------------------------------------------
# create minerals subset, eg mineral_id is mineral, parent_id is serie, root, subgroup etc ----------------------------------------
minerals <- initial %>%
  filter(!is.na(mineral_name))

minerals_series <- minerals %>%
  select(mineral_name, serie) %>%
  filter(!is.na(serie)) %>%
  select(mineral_name, serie) %>%
  distinct()

minerals_roots <- minerals %>%
  select(mineral_name, serie, root) %>%
  filter(is.na(serie) & !is.na(root)) %>%
  select(mineral_name, root) %>%
  distinct()

minerals_subgroup <- minerals %>%
  select(mineral_name, serie, root, subgroup) %>%
  filter(is.na(serie) & is.na(root) & !is.na(subgroup)) %>%
  select(mineral_name, subgroup) %>%
  distinct()

minerals_group <- minerals %>%
  select(mineral_name, serie, root, subgroup, group) %>%
  filter(is.na(serie) & is.na(root) & is.na(subgroup) & !is.na(group)) %>%
  select(mineral_name, group) %>%
  distinct()

minerals_supergroup <- minerals %>%
  select(mineral_name, serie, root, subgroup, group, supergroup) %>%
  filter(is.na(serie) & is.na(root) & is.na(subgroup) & is.na(group) & !is.na(supergroup)) %>%
  distinct()

mineral_hierarchy <- initial %>%
  select(Mineral_Name, LOCALITY, Type, Note) %>%
  filter(!is.na(LOCALITY)) %>%
  left_join(locality_type_list, by = c('Type' = 'locality_type_name'), copy = TRUE) %>%
  select(-c( note)) %>%
  left_join(mineral_list, by=c('Mineral_Name'='mineral_name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # COMPARE MINERALS WITH MASTER TABLE !
  select(mineral_id, LOCALITY, locality_type_id, Note) %>%
  rename(locality_name=LOCALITY, note=Note)


# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_hierarchy RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_hierarchy", mineral_hierarchy, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
