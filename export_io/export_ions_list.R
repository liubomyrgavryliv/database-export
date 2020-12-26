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


ion_class_list = tbl(conn, 'ion_class_list')
ion_subclass_list = tbl(conn, 'ion_subclass_list')
ion_group_list = tbl(conn, 'ion_group_list')
ion_subgroup_list = tbl(conn, 'ion_subgroup_list')
ion_type_list = tbl(conn, 'ion_type_list')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Ions_data',
  range = 'A:U',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse anions ----------------------------------------------------------------

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ions_list;")
dbWriteTable(conn, "ions_list", ions_list, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(ions_list, path = paste0(path, 'ions_list.csv'), na='')
