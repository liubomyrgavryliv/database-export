library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  #range = 'A:AY',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# minerals named after flowers 02/03/21
stats <- initial %>%
  filter_at(.vars = vars(Person, Role, LOCALITY, Note, Meaning, LANGUAGE, Stem1, Stem2, Stem3, CHEMISTRY, other, note),
            .vars_predicate = any_vars(str_detect(. , 'flower'))) %>%
  select(Mineral_Name, discovery_year_min, discovery_country, Meaning, LANGUAGE, Stem1, Stem2, Stem3, other, note)


# Export data

write_csv(stats, 'stats.csv', na='', quote_escape = "double")
