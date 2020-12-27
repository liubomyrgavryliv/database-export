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

ion_list <- ion_list %>% arrange(-ion_type_id)

# check impurities
mineral_impurity <- initial %>%
  select(Impurities, Content) %>%
  filter(!is.na(Impurities)) %>%
  mutate(Impurities = str_split(Impurities, '\\,\\ |\\;|,')) %>%
  unchop(Impurities, keep_empty = TRUE) %>% 
  distinct(Impurities) %>%
  arrange(Impurities) %>%
  left_join(ion_list, by=c('Impurities' = 'formula'), copy=TRUE) %>%
  filter(is.na(ion_id))

# parse data 
mineral_impurity <- initial %>%
  select(Mineral_Name, Impurities, Content) %>%
  filter(!is.na(Impurities)) %>%
  mutate(ion_quantity=ifelse(str_detect(Content,'[0-9]'),Content, NA),
         rich_poor=ifelse(str_detect(Content,'\\([\\+\\-]'),Content, NA)) %>%
  mutate(ion_quantity = str_split(ion_quantity, '\\,\\ |\\;|,'),
         rich_poor = str_split(rich_poor, '\\,\\ |\\;|,'),
         Impurities = str_split(Impurities, '\\,\\ |\\;|,')) %>%
  unchop(c('Impurities', 'ion_quantity', 'rich_poor'), keep_empty = TRUE) %>%
  distinct(Mineral_Name, Impurities, ion_quantity, rich_poor) %>%
  # filter(Mineral_Name == 'High-Hydrated Si-Deficient Vesuvianite') # For check
  mutate(rich_poor=ifelse(str_detect(rich_poor,'\\+'), 1, rich_poor)) %>%
  mutate(rich_poor=ifelse(str_detect(rich_poor,'\\-'), 0, rich_poor)) %>%
  rename(mineral_name=Mineral_Name) %>%
  left_join(mineral_list, by='mineral_name', copy=TRUE) %>%
  left_join(ion_list, by=c('Impurities' = 'formula'), copy=TRUE) %>%
  select(mineral_id, ion_id, ion_quantity, rich_poor)


# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_impurity RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_impurity", mineral_impurity, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
