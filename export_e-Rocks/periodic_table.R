library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

# Upload and filter data sheets

status_data <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:D',
  col_names = TRUE,
  na = ""
) %>%
  filter(str_detect(all_indexes, '0.0')) %>%
  select(Mineral_Name, IMA.Status)


nickel_strunz <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:N',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, all_indexes, IMA.Status) %>%
  filter(str_detect(all_indexes, '0.0'))

names <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  range = 'A:B',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, discovery_year_min)

# parse and merge data

periodic_table <- nickel_strunz %>%
  


name
cation - seperated by ;
anion - seperated by ;
silicate - seperated by ;
silicate_html -> html rendering of silicate for user to read
cation_html -> html rendering of cation for user to read
anion_html -> html rendering of anion for user to read
elements
strunz
formula
hydrate
hydroxyl
ima
year