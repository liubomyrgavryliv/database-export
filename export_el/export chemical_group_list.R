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
chemical_group_list <- initial %>%
  select(group_block) %>%
  rename(chemical_group_name = group_block) %>%
  distinct(chemical_group_name) %>%
  filter(!is.na(chemical_group_name)) %>%
  arrange(chemical_group_name)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE chemical_group_list RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "chemical_group_list", chemical_group_list, append=TRUE)


dbDisconnect(conn)
