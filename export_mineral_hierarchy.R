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
  filter(!is.na(serie)) %>%
  distinct(mineral_name, serie, .keep_all = TRUE) %>%
  mutate(is_top_level = ifelse(is.na(root) & is.na(subgroup) & is.na(group) & is.na(supergroup), 1, 0)) %>%
  select(mineral_name, serie, is_top_level) %>%
  rename(parent_id=serie)
  # group_by(mineral_name, serie, is_top_level) %>%
  # filter(n() > 1)

minerals_roots <- minerals %>%
  filter(is.na(serie) & !is.na(root)) %>%
  mutate(is_top_level = ifelse(is.na(subgroup) & is.na(group) & is.na(supergroup), 1, 0)) %>%
  select(mineral_name, root, is_top_level) %>%
  distinct() %>%
  rename(parent_id=root)

minerals_subgroup <- minerals %>%
  filter(is.na(serie) & is.na(root) & !is.na(subgroup)) %>%
  mutate(is_top_level = ifelse(is.na(group) & is.na(supergroup), 1, 0)) %>%
  select(mineral_name, subgroup, is_top_level) %>%
  distinct() %>%
  rename(parent_id=subgroup)

minerals_group <- minerals %>%
  filter(is.na(serie) & is.na(root) & is.na(subgroup) & !is.na(group)) %>%
  mutate(is_top_level = ifelse(is.na(supergroup), 1, 0)) %>%
  select(mineral_name, group, is_top_level) %>%
  distinct() %>%
  rename(parent_id=group)

minerals_supergroup <- minerals %>%
  select(mineral_name, serie, root, subgroup, group, supergroup) %>%
  filter(is.na(serie) & is.na(root) & is.na(subgroup) & is.na(group) & !is.na(supergroup)) %>%
  mutate(is_top_level = 1) %>%
  select(mineral_name, supergroup, is_top_level) %>%
  distinct() %>%
  rename(parent_id=supergroup)

minerals <- rbind(minerals_series, minerals_roots, minerals_subgroup, minerals_group, minerals_supergroup)
# create series subset ------------------------------------------------------------------------

series <- initial %>%
  filter(!is.na(serie))

series_root <- series %>%
  filter(!is.na(serie)) %>%
  distinct(mineral_name, serie, .keep_all = TRUE) %>%
  mutate(is_top_level = ifelse(is.na(root) & is.na(subgroup) & is.na(group) & is.na(supergroup), 1, 0)) %>%
  select(mineral_name, serie, is_top_level)
# group_by(mineral_name, serie, is_top_level) %>%
# filter(n() > 1)




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
