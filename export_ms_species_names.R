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
  range = 'A:AY',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

ms_species_discovery <- initial %>%
  select(!c('Sorted by',Prefix_Language,Prefix_Chemistry,'Root Mineral Name',Suffix_Language,
            Suffix_Chemistry,Suffix_Person, `Mindat ID`, ISO, Country_ISO, `IMA (CNMNC) (DANA)`, IMA.Status, Status,
            NAMES_DATA, CLASS)) %>%
  rename(name = Mineral_Name, discovery_year = `Named_date/Year_published`,
         discovery_country = 'Country of Discovery', first_usage_date = First_usage_date) %>%
  mutate(discovery_year = ifelse(str_detect(discovery_year, '[Uu]nknown'), NA, discovery_year),
         discovery_country = ifelse(str_detect(discovery_country, '[Uu]nknown'), NA, discovery_country))
