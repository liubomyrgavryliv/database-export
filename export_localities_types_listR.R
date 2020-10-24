library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/post-doc/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  range = 'A:AV',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# PROCESS DATA -----------------------------------------------------------------
locality_type_list <- initial %>%
  select(Type) %>%
  filter(!is.na(Type)) %>%
  distinct(Type)

# EXPORT ms_species ------------------------------------------------------------
write_csv(locality_type_list, path = paste0(path, 'locality_type_list.csv'), na='')
