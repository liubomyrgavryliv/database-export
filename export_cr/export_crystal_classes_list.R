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

crystal_systems_list <- tbl(conn, 'crystal_systems_list')
# PROCESS data -----------------------------------------------------------------
crystal_classes <-
  initial %>%
  select(`Class Name`,`H-M symbol`,`Crystal System`) %>%
  rename(crystal_class_name=`Class Name`,h_m_symbol=`H-M symbol`, crystal_system_name=`Crystal System`) %>%
  inner_join(crystal_systems_list, by='crystal_system_name', copy=TRUE) %>%
  select(crystal_class_name,h_m_symbol,crystal_system_id) %>%
  mutate(h_m_symbol=as.character(h_m_symbol)) %>%
  distinct() %>%
  arrange(crystal_class_name)

# LOAD into DB
dbSendQuery(conn, "drop table if exists crystal_classes_list;")
dbWriteTable(conn, "crystal_classes_list", crystal_classes, append=TRUE)

dbDisconnect(conn)