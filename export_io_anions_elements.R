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


io_anions = tbl(conn, 'io_anions')
el_data = tbl(conn, 'el_data')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Anions',
  range = 'A:J',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse anions ----------------------------------------------------------------
io_anions_elements <- initial %>%
  filter(!is.na(`Name (short name)`)) %>% # REMOVE AFTER VITALII REMOVES H !! -----
  distinct(Ion, .keep_all = TRUE) %>%
  select(Ion) %>%
  mutate(elements = str_extract_all(Ion, '[A-Z][a-z]?')) %>%
  mutate(elements = map(elements, function(x){  return(unique(unlist(x))) })) %>%
  left_join(io_anions, by=c('Ion' = 'formula'), copy=TRUE) %>%
  unchop(elements, keep_empty=FALSE) %>%
  left_join(el_data, by=c('elements' = 'element'), copy=TRUE) %>%
  select(anion_id, element_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_anions_elements;")
dbWriteTable(conn, "io_anions_elements", io_anions_elements, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_anions_elements, path = paste0(path, 'io_anions_elements.csv'), na='')
