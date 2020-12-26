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
io_types_list <- 
  tibble(ion_type_id=1:4,
         ion_type_name=c('Anion', 'Cation', 'Silicate', 'Other'))


# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_types_list;")
dbWriteTable(conn, "io_types_list", io_types_list, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_types_list, path = paste0(path, 'io_types_list.csv'), na='')
