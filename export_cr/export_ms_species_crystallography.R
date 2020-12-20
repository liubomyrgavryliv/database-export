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
  
  # filter(str_detect(`Crystal System`, 'trigonal')) %>%
  # filter(a==b && a==c)

  

# LOAD into DB
dbSendQuery(conn, "delete from ns_space_groups_list;")
dbWriteTable(conn, "ns_space_groups_list", ns_space_groups, append=TRUE)

dbDisconnect(conn)
