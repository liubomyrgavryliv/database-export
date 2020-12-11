library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- '../export/'

# LOAD data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1XDtvw5isb6lzAMrhdXJfP32LlKte94momwj0kMqRXDg',
  sheet = 'Sheet2',
  range = 'A:E',
  col_names = TRUE,
  na = "cccccc"
) %>% select(`Space group`,`Non-standard space group`,`H-M symbol`,`Class Name`,`Crystal System`)

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

crystal_classes_list <- tbl(conn, 'crystal_classes_list')
# PROCESS data -----------------------------------------------------------------
space_groups <-
  initial %>%
  select(`Space group`,`Class Name`) %>%
  rename(space_group_name=`Space group`,crystal_class_name=`Class Name`) %>%
  inner_join(crystal_classes_list, by='crystal_class_name', copy=TRUE) %>%
  select(space_group_name,crystal_class_id) %>%
  distinct() %>%
  arrange(space_group_name)

# LOAD into DB
dbSendQuery(conn, "delete from space_groups_list;")
dbWriteTable(conn, "space_groups_list", space_groups, append=TRUE)

dbDisconnect(conn)
