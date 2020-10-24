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


ms_species = tbl(conn, 'ms_species')
io_anions = tbl(conn, 'io_anions')
#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:I',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

io_anions <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Anions',
  range = 'A:E',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# unique anions in ms_anions_theoretical
anions_unique <- 
  initial %>%
  select(anions_theoretical) %>%
  mutate(anions_theoretical = str_split(anions_theoretical, ';')) %>%
  unchop(anions_theoretical, keep_empty = TRUE) %>%
  distinct(anions_theoretical) %>%
  arrange(anions_theoretical)


# compare unique 'theoretical' anions with those stored in ions_table
anions_absent <-
  anions_unique %>%
  anti_join(io_anions, by = c('anions_theoretical' = 'Ion'), copy=TRUE)

io_absent <-
  io_anions %>%
  anti_join(anions_unique, by = c('Ion' = 'anions_theoretical'))

# create ms_anions_theoretical
ms_anions_theoretical <- initial %>%
  select(Mineral_Name, anions_theoretical) %>%
  mutate(anions_theoretical = str_split(anions_theoretical, ';')) %>%
  unchop(anions_theoretical, keep_empty = TRUE) %>%
  filter(!is.na(anions_theoretical)) %>%
  left_join(ms_species, by=c('Mineral_Name' = 'mineral_name'), copy=TRUE) %>%
  # filter(is.na(mineral_id)) # CHECK if all minerals are added to ms_species !
  left_join(io_anions, by=c('anions_theoretical' = 'formula'), copy=TRUE) %>%
  # filter(is.na(anion_id)) # CHECK if all anions are added to io_anions !
  select(mineral_id, anion_id)

# EXPORT DATA ------------------------------------------------------------------
write_csv(ns_anions_unique, path = paste0(path, 'ns_anions_unique.csv'), na='')
write_csv(anions_absent, path = paste0(path, 'anions_absent.csv'), na='')
write_csv(ions_duplicates, path = paste0(path, 'ions_duplicates.csv'), na='')
write_csv(io_absent, path = paste0(path, 'io_absent.csv'), na='')

