library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

io_positions_list = tbl(conn, 'io_positions_list')
ms_species = tbl(conn, 'ms_species')
#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'GROUPS_psql',
  range = 'A:S',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

ms_species_ions <-
  initial %>%
  inner_join(ms_species, by=c('Name' = 'mineral_name'), copy=TRUE) # , Lepidolite Series OMIT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_species_ions;")
dbWriteTable(conn, "ms_species_ions", ms_species_ions, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_positions_list, file = paste0(path, 'io_positions_list.csv'), na='')







