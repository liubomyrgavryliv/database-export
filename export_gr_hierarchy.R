library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


# read Groups data
initial <- googlesheets4::read_sheet(
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'Groups_formulae_table',
  range = 'B:I',
  col_names = TRUE,
  col_types = 'cccccccc',
  na = ""
)

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

ms_species <- tbl(conn, 'ms_species')
# ------------------------------------------------------------------------------

gr_hierarchy <- initial %>%
  select(c(Supergroup, Group, Subgroup, Aliases, Series, Minerals_Names)) %>%
  filter(is.na(Group) | !str_detect(Group, '^\"')) %>%
  filter(!is.na(Supergroup) | !is.na(Group) | !is.na(Subgroup) | !is.na(Aliases) | !is.na(Series) | !is.na(Minerals_Names)) %>%
  mutate(Aliases = ifelse(!is.na(Aliases), paste0(Aliases, ' Root'), NA)) %>%
  rename(c(Root = Aliases)) %>%
  left_join(ms_species, by=c('Supergroup'='mineral_name'), copy=TRUE) %>%
  select(mineral_id, Group, Subgroup, Root, Series, Minerals_Names) %>%
  rename(supergroup_id = mineral_id) %>%
  left_join(ms_species, by=c('Group'='mineral_name'), copy=TRUE) %>%
  select(supergroup_id, mineral_id, Subgroup, Root, Series, Minerals_Names) %>%
  rename(group_id = mineral_id) %>%
  left_join(ms_species, by=c('Subgroup'='mineral_name'), copy=TRUE) %>%
  select(supergroup_id, group_id, mineral_id, Root, Series, Minerals_Names) %>%
  rename(subgroup_id = mineral_id) %>%
  left_join(ms_species, by=c('Root'='mineral_name'), copy=TRUE) %>%
  select(supergroup_id, group_id, subgroup_id, mineral_id, Series, Minerals_Names) %>%
  rename(root_id = mineral_id) %>%
  left_join(ms_species, by=c('Series'='mineral_name'), copy=TRUE) %>%
  select(supergroup_id, group_id, subgroup_id, root_id, mineral_id, Minerals_Names) %>%
  rename(serie_id = mineral_id) %>%
  left_join(ms_species, by=c('Minerals_Names'='mineral_name'), copy=TRUE) %>%
  select(supergroup_id, group_id, subgroup_id, root_id, serie_id, mineral_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM gr_hierarchy;")
dbWriteTable(conn, "gr_hierarchy", gr_hierarchy, append=TRUE)
dbDisconnect(conn)

# SAVE GROUPS
write_csv(gr_hierarchy, path = paste0(path, 'gr_hierarchy.csv'), na='')


