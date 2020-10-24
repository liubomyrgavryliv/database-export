library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/post-doc/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'ms_names_other',
  #range = 'A:AY',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')


# LOAD DATA -------------------------------------------------------------------

names_other_list <- 
  initial %>%
  select(other) %>%
  filter(!is.na(other)) %>%
  distinct() %>%
  rename(type = other) %>%
  mutate(note = NA) %>%
  add_row(type='Chemistry', note=NA) %>%
  arrange(type)


# UPLOAD DATA TO DB ------------------------------------------------------------
dbSendQuery(conn, "DELETE FROM names_other_list;")
dbWriteTable(conn, "names_other_list", names_other_list, append=TRUE)
dbDisconnect(conn)

# EXPORT DATA ------------------------------------------------------------------
write_csv(names_other_list, path = paste0(path, 'names_other_list.csv'), na='')

# disconnect from DB
dbDisconnect(conn)
