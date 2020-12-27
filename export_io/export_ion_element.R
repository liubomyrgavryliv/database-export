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
element_list = tbl(conn, 'element_list')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Ions_data',
  range = 'A:H',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse ions ----------------------------------------------------------------
ion_list_local <- 
  ion_list %>% 
            collect()

ion_element <- ion_list_local %>%
  select(ion_id, formula) %>%
  mutate(element=str_match_all(formula, '[A-Z][a-z]?')) %>%
  mutate(element=ifelse(formula=='REE', list(c('Ce', 'Dy', 'Er', 'Eu', 'Gd', 'Ho', 'La', 'Lu', 'Nd', 'Pr', 'Pm', 'Sm', 'Sc', 'Tb', 'Tm', 'Yb', 'Y')), element)) %>%
  mutate(element=ifelse(formula=='Ln^3+^', list(c('Ce', 'Dy', 'Er', 'Gd', 'La', 'Nd', 'Pr', 'Sm', 'Yb')), element)) %>%
  unchop(element, keep_empty = TRUE) %>%
  distinct(ion_id, element) %>%
  left_join(element_list, by='element', copy=TRUE) %>%
  select(ion_id, element_id)



# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE ion_element RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "ion_element", ion_element, append=TRUE)

# Disconnect from the DB
dbDisconnect(conn)
