library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'


#Load data ---------------------------------------------------------------------
names <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  #range = 'A:AY',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

status <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:D',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

ns <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) %>%
  select(Mineral_Name, Index, Formula, Strunz)

# minerals named after flowers 02/03/21
stats <- initial %>%
  filter_at(.vars = vars(Person, Role, LOCALITY, Note, Meaning, LANGUAGE, Stem1, Stem2, Stem3, CHEMISTRY, other, note),
            .vars_predicate = any_vars(str_detect(. , 'flower'))) %>%
  select(Mineral_Name, discovery_year_min, discovery_country, discovery_country_note, Meaning, LANGUAGE, Stem1, Stem2, Stem3, other, note)

# minerals which include PGE
stats <- ns %>%
  left_join(status, by=c('Mineral_Name')) %>%
  filter(str_detect(Formula, 'Ru|Rh|Pt|Os|Ir|Pd')) %>%
  select(Mineral_Name, Formula, all_indexes) %>%
  filter(str_detect(all_indexes, '0.0'))

# minerals named after F
stats <- names %>%
  left_join(status, by=c('Mineral_Name')) %>%
  # filter(str_detect(all_indexes, '0.0')) %>%
  filter(str_detect(Gender, 'F')) %>%
  filter(!str_detect(Gender, 'M')) %>%
  select(Mineral_Name)
  filter(discovery_year_min > 2015) %>%
  arrange(desc(discovery_year_min)) %>%
  select(Mineral_Name, discovery_year_min, Person, Gender, Role, Nationality) %>%
  mutate(Nationality = str_split(Nationality, ';')) %>%
  unchop(Nationality, keep_empty = T) %>%
  group_by(discovery_year_min) %>%
  summarise(minerals = n_distinct(Mineral_Name)) %>%
  arrange(desc(discovery_year_min))
  # filter(str_detect(all_indexes, '0.0'))

# minerals with biggest formulas
  stats <- ns %>%
    left_join(status, by=c('Mineral_Name')) %>%
    mutate(elements = str_match_all(Formula, '[A-Z][a-z]?')) %>%
    unchop(elements, keep_empty = TRUE) %>%
    distinct(Mineral_Name, Formula, elements) %>%
    group_by(Mineral_Name) %>%
    summarise(elements=list(elements)) %>%
    filter(!is.na(elements)) %>%
    rowwise() %>%
    mutate(counts=length(elements)) %>%
    arrange(desc(counts))
  
  stats <- stats %>%
    filter(counts == 4)
  
  stats %>%
    group_by(counts) %>%
    summarise(count=length(counts))
  
# minerals discovered by Ireland
  stats <- names %>%
    left_join(status, by=c('Mineral_Name')) %>%
    filter(str_detect(discovery_country, '^Ireland') | str_detect(Nationality, 'Irish'))
  
# Export data

write_csv(stats, 'stats.csv', na='', quote_escape = "double")
