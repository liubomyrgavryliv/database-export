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


ion_list = tbl(conn, 'ion_list')
mineral_list = tbl(conn, 'mineral_list')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:R',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse ions ----------------------------------------------------------------
mineral_impurity <- initial %>%
  select(Impurities, Content) %>%
  filter(!is.na(Impurities))


# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_impurity RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_impurity", mineral_impurity, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
