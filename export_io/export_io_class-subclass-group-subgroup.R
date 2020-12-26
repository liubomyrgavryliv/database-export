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


#Load data ---------------------------------------------------------------------

initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Ions_data',
  range = 'A:U',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 
  
# Parse classes ----------------------------------------------------------------
ion_class_list <- initial %>%
  select(ion_class_name) %>%
  distinct(ion_class_name) %>%
  arrange(ion_class_name) %>%
  mutate(ion_class_id = row_number()) %>%
  select(ion_class_id, ion_class_name)

ion_subclass_list <- initial %>%
  select(ion_subclass_name) %>%
  distinct(ion_subclass_name) %>%
  filter(!is.na(ion_subclass_name)) %>%
  arrange(ion_subclass_name) %>%
  mutate(ion_subclass_id = row_number()) %>%
  select(ion_subclass_id, ion_subclass_name)

ion_group_list <- initial %>%
  select(ion_group_name) %>%
  distinct(ion_group_name) %>%
  filter(!is.na(ion_group_name)) %>%
  arrange(ion_group_name) %>%
  mutate(ion_group_id = row_number()) %>%
  select(ion_group_id, ion_group_name)

ion_subgroup_list <- initial %>%
  select(ion_subgroup_name) %>%
  distinct(ion_subgroup_name) %>%
  filter(!is.na(ion_subgroup_name)) %>%
  arrange(ion_subgroup_name) %>%
  mutate(ion_subgroup_id = row_number()) %>%
  select(ion_subgroup_id, ion_subgroup_name)
  

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ion_class_list WHERE 1=1;")
dbWriteTable(conn, "ion_class_list", ion_class_list, append=TRUE)

dbSendQuery(conn, "DELETE FROM ion_subclass_list;")
dbWriteTable(conn, "ion_subclass_list", ion_subclass_list, append=TRUE)

dbSendQuery(conn, "DELETE FROM ion_group_list;")
dbWriteTable(conn, "ion_group_list", ion_group_list, append=TRUE)

dbSendQuery(conn, "DELETE FROM ion_subgroup_list;")
dbWriteTable(conn, "ion_subgroup_list", ion_subgroup_list, append=TRUE)
dbDisconnect(conn)

