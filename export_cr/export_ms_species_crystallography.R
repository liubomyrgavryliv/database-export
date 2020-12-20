library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- '../export/'

# LOAD data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Crystallography',
  range = 'A:O',col_types = 'c',
  col_names = TRUE,
)

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

space_groups_list <- tbl(conn, 'space_groups_list')
ns_space_groups_list <- tbl(conn, 'ns_space_groups_list')
crystal_systems_list <- tbl(conn, 'crystal_systems_list')
crystal_classes_list <- tbl(conn, 'crystal_classes_list')
ms_species <- tbl(conn, 'ms_species')
# PROCESS data -----------------------------------------------------------------
# validate crystal_system_name
validate <- initial %>%
  select(`Crystal System`) %>%
  distinct(`Crystal System`) %>%
  left_join(crystal_systems_list,by = c('Crystal System' = 'crystal_system_name'), copy=TRUE) %>%
  filter(!is.na('Crystal System') && is.na(crystal_system_id)) # check if all crystal systems are present

# validate crystal_class_name
validate <- initial %>%
  select(`Class Name`) %>%
  mutate(crystal_class_name=str_split(`Class Name`, ',|;|, ')) %>%
  unchop(crystal_class_name, keep_empty = TRUE) %>%
  distinct(crystal_class_name) %>%
  arrange(crystal_class_name) %>%
  left_join(crystal_classes_list,by = 'crystal_class_name', copy=TRUE) %>%
  filter(!is.na('Crystal Class') && is.na(crystal_class_id)) # check if all crystal classes are present

# validate space_group_name and ns_space_group_name
validate <- initial %>%
  select(Mineral_Name,`Space group`, Note) %>%
  filter(str_detect(Note, 'standard')) %>%
  mutate(ns_space_group_name=str_split(`Space group`, ', |;|,')) %>%
  unchop(ns_space_group_name, keep_empty = TRUE) %>%
  left_join(space_groups_list, by=c('ns_space_group_name'='space_group_name'), copy=TRUE) %>%
  filter(is.na(space_group_id)) %>%
  left_join(ns_space_groups_list, by='ns_space_group_name', copy=TRUE) %>%
  filter(is.na(ns_space_group_id))
  
# parse a, b, c, alpha, beta, gamma to numeric
ms_species_crystal <- initial %>%
  filter(!is.na(`Crystal System`)) %>%
  mutate(a=as.numeric(str_replace_all(a,'\\(.*\\)', '')),
         b=as.numeric(str_replace_all(b,'\\(.*\\)', '')),
         c=as.numeric(str_replace_all(c,'\\(.*\\)', '')),
         α=as.numeric(str_replace_all(α,'\\(.*\\)', '')),
         β=as.numeric(str_replace_all(β,'\\(.*\\)', '')),
         γ=as.numeric(str_replace_all(γ,'\\(.*\\)', ''))
         ) %>%
  mutate(space_group_name=str_split(`Space group`, ', |;|,')) %>%
  unchop(space_group_name, keep_empty = TRUE) %>%
  left_join(crystal_systems_list, by=c('Crystal System' = 'crystal_system_name'), copy=TRUE) %>%
  left_join(space_groups_list, by='space_group_name', copy=TRUE) %>%
  left_join(ns_space_groups_list, by=c('space_group_name'='ns_space_group_name'), copy=TRUE) %>%
  rename(space_group_id=space_group_id.x, mineral_name=`Mineral_Name`, alpha=α, beta=β, gamma=γ, z=Z) %>%
  mutate(space_group_id=ifelse(!is.na(ns_space_group_id), space_group_id.y, space_group_id)) %>%
  inner_join(ms_species, by='mineral_name', copy=TRUE) %>%
  select(mineral_id, crystal_system_id, crystal_class_id, space_group_id, ns_space_group_id, a, b, c, alpha, beta, gamma, z)


# LOAD into DB
dbSendQuery(conn, "delete from ms_species_crystal;")
dbWriteTable(conn, "ms_species_crystal", ms_species_crystal, append=TRUE)

dbDisconnect(conn)
