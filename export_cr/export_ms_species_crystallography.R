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
  range = 'A:O',
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
ms_species_crystal <-
  initial %>%
  select(`Class Name`) %>%
  mutate(crystal_class_name=str_split(`Class Name`, ',|;|, ')) %>%
  unchop(crystal_class_name, keep_empty = TRUE) %>%
  distinct(crystal_class_name) %>%
  arrange(crystal_class_name) %>%
  left_join(crystal_classes_list,by = 'crystal_class_name', copy=TRUE) %>%
  filter(is.na('Crystal System') & is.na(crystal_system_id)) # check if all crystal classes are present

  # left_join(crystal_systems_list,by = c('Crystal System' = 'crystal_system_name'), copy=TRUE) %>%
  # filter(is.na('Crystal System') & is.na(crystal_system_id)) # check if all crystal systems are present
  

# LOAD into DB
dbSendQuery(conn, "delete from ns_space_groups_list;")
dbWriteTable(conn, "ns_space_groups_list", ns_space_groups, append=TRUE)

dbDisconnect(conn)
