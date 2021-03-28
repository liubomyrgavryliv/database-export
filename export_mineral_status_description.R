library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'gpmineralsDB', 
                  host = 'localhost',
                  port = 5432,
                  user = 'gpminerals',
                  password = 'gpmineralsDB')


mineral_list = tbl(conn, 'mineral_list')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:Y',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse mineral impurity ----------------------------------------------------------------

impurity <- initial %>%
  filter(!is.na(Impurities)) %>%
  select(Mineral_Name, Impurities, Content) %>%
  mutate(Impurities = str_split(Impurities, '\\,\\ |\\;|,'),
         Content = str_split(Content, '\\,\\ |\\;|,')) %>%
  unchop(cols=c('Impurities', 'Content'), keep_empty = TRUE) %>% 
  distinct(Impurities) %>%
  arrange(Impurities) %>%


# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_status_description RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_status_description", mineral_status_description, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
