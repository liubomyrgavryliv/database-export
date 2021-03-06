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
  na = ""
) %>% select(`Space group`,`Non-standard space group`,`H-M symbol`,`Class Name`,`Crystal System`)

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

# PROCESS data -----------------------------------------------------------------
crystal_systems <-
  initial %>%
  select(`Crystal System`) %>%
  rename(crystal_system_name=`Crystal System`) %>%
  distinct() %>%
  arrange()

# LOAD into DB
dbSendQuery(conn, "DELETE FROM crystal_systems_list;")
dbWriteTable(conn, "crystal_systems_list", crystal_systems, append=TRUE)

dbDisconnect(conn)