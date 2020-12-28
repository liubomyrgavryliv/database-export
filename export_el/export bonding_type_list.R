library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Elements_data',
  range = 'A:AV',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# PROCESS DATA -----------------------------------------------------------------
bonding_type_list <- initial %>%
  select(bonding_type) %>%
  rename(bonding_type_name = bonding_type) %>%
  distinct(bonding_type_name) %>%
  filter(!is.na(bonding_type_name)) %>%
  arrange(bonding_type_name)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE bonding_type_list RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "bonding_type_list", bonding_type_list, append=TRUE)


dbDisconnect(conn)
