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

space_groups_list <- tbl(conn, 'space_groups_list')
# PROCESS data -----------------------------------------------------------------
ns_space_groups <-
  initial %>%
  select(`Space group`,`Non-standard space group`) %>%
  filter(!is.na(`Non-standard space group`)) %>%
  rename(ns_space_group_name=`Non-standard space group`, space_group_name=`Space group`) %>%
  inner_join(space_groups_list, by='space_group_name', copy=TRUE) %>%
  select(ns_space_group_name,space_group_id) %>%
  mutate(ns_space_group_name=str_split(ns_space_group_name, ', ')) %>%
  unchop(ns_space_group_name, keep_empty = TRUE) %>%
  arrange(ns_space_group_name)

# LOAD into DB
dbSendQuery(conn, "delete from ns_space_groups_list;")
dbWriteTable(conn, "ns_space_groups_list", ns_space_groups, append=TRUE)

dbDisconnect(conn)
