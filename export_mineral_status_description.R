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
  mutate(Content = ifelse(Content == '(+)', 'rich', Content)) %>%
  mutate(Content = ifelse(Content == '(-)', 'poor', Content)) %>%
  mutate(Content = ifelse(str_detect(Content, '[0-9]'), paste0(Content, ' %'), Content)) %>%
  group_by(Mineral_Name) %>%
  summarise(test = ifelse(length(unique(Content)) == 1, 
                          # ifelse(length(Impurities) > 1, paste0('(', paste0(Impurities, collapse = ', '), ') ', Content), paste0(Impurities,' ',Content)),
                          paste0(paste0(Impurities, collapse = '-'), ' ', Content),
                          paste0(Impurities,' ', Content, collapse = ', ')))


# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE mineral_status_description RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "mineral_status_description", mineral_status_description, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
