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
# check duplicates
check <- initial %>%
  select(supergroup, group, subgroup, root, serie, mineral_name) %>%
  group_by(mineral_name) %>%
  filter(n() > 1) %>%
  distinct() %>%
  arrange(mineral_name)


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
  filter(!is.na(root)) %>%
  mutate(is_top_level = ifelse(is.na(subgroup) & is.na(group) & is.na(supergroup), 1, 0)) %>%
  select(serie, root, is_top_level) %>%
  distinct() %>%
  rename(parent_id=root, mineral_name=serie)

series_subgroup <- series %>%
  filter(is.na(root) & !is.na(subgroup)) %>%
  mutate(is_top_level = ifelse(is.na(group) & is.na(supergroup), 1, 0)) %>%
  select(serie, subgroup, is_top_level) %>%
  distinct() %>%
  rename(parent_id=subgroup, mineral_name=serie)

series_group <- series %>%
  filter(is.na(root) & is.na(subgroup) & !is.na(group)) %>%
  mutate(is_top_level = ifelse(is.na(supergroup), 1, 0)) %>%
  select(serie, group, is_top_level) %>%
  distinct() %>%
  rename(parent_id=group, mineral_name=serie)

series_supergroup <- series %>%
  filter(is.na(root) & is.na(subgroup) & is.na(group) & !is.na(supergroup)) %>%
  mutate(is_top_level = 1) %>%
  select(serie, supergroup, is_top_level) %>%
  distinct() %>%
  rename(parent_id=supergroup, mineral_name=serie)

series <- rbind(series_root, series_root, series_subgroup, series_group, series_supergroup)

# create roots subset ------------------------------------------------------------------------

roots <- initial %>%
  filter(!is.na(root))

root_subgroup <- roots %>%
  filter(!is.na(subgroup)) %>%
  mutate(is_top_level = ifelse(is.na(group) & is.na(supergroup), 1, 0)) %>%
  select(root, subgroup, is_top_level) %>%
  distinct() %>%
  rename(parent_id=subgroup, mineral_name=root)

root_group <- roots %>%
  filter(is.na(subgroup) & !is.na(group)) %>%
  mutate(is_top_level = ifelse(is.na(supergroup), 1, 0)) %>%
  select(root, group, is_top_level) %>%
  distinct() %>%
  rename(parent_id=group, mineral_name=root)

root_supergroup <- roots %>%
  filter(is.na(subgroup) & is.na(group) & !is.na(supergroup)) %>%
  mutate(is_top_level = 1) %>%
  select(root, supergroup, is_top_level) %>%
  distinct() %>%
  rename(parent_id=supergroup, mineral_name=root)

roots <- rbind(root_subgroup, root_group, root_supergroup)
# create subgroup subset ------------------------------------------------------------------------

subgroups <- initial %>%
  filter(!is.na(subgroup))

subgroup_group <- subgroups %>%
  filter(!is.na(group)) %>%
  mutate(is_top_level = ifelse(is.na(supergroup), 1, 0)) %>%
  select(subgroup, group, is_top_level) %>%
  distinct() %>%
  rename(parent_id=group, mineral_name=subgroup)

subgroup_supergroup <- subgroups %>%
  filter(is.na(group) & !is.na(supergroup)) %>%
  mutate(is_top_level = 1) %>%
  select(subgroup, supergroup, is_top_level) %>%
  distinct() %>%
  rename(parent_id=supergroup, mineral_name=subgroup)

subgroups <- rbind(subgroup_group, subgroup_supergroup)

# create group subset ------------------------------------------------------------------------

groups <- initial %>%
  filter(!is.na(group))

group_supergroup <- groups %>%
  filter(!is.na(supergroup)) %>%
  mutate(is_top_level = 1) %>%
  select(supergroup, group, is_top_level) %>%
  distinct() %>%
  rename(parent_id=supergroup, mineral_name=group)

hierarchy <- rbind(minerals, series, roots, subgroups, group_supergroup)

# Create subset with parent_id - NULL values
parents <- hierarchy %>%
  filter(is_top_level == 1) %>%
  rename(parent_id=mineral_name, mineral_name=parent_id) %>%
  mutate(parent_id=NA) %>%
  select(mineral_name, parent_id) %>%
  distinct(mineral_name, .keep_all = T)
  
  # FINAL SUBSET
  mineral_hierarchy <-
    hierarchy %>%
    select(!is_top_level) %>%
    rbind(parents) %>%
    inner_join(mineral_list, by=c('mineral_name' = 'mineral_name'), copy=TRUE) %>%
    left_join(mineral_list, by=c('parent_id' = 'mineral_name'), copy=TRUE) %>%
    select(mineral_id.x, mineral_id.y) %>%
    rename(mineral_id=mineral_id.x, parent_id=mineral_id.y) %>%
    distinct()

  mineral_hierarchy %>%
    filter(mineral_id == '2a1a1e9a-ac5d-4356-bb09-34d543460e61')

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_hierarchy RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_hierarchy", mineral_hierarchy, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
