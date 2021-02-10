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
  select(Mineral_Name, all_indexes, IMA.Status)


nickel_strunz <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:N',
  col_names = TRUE,
  na = ""
)

names <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  range = 'A:B',
  col_names = TRUE,
  col_types = 'cc',
  na = ""
) %>%
  select(Mineral_Name, discovery_year_min)

# parse and merge data

periodic_table <- nickel_strunz %>%
  mutate(elements = str_match_all(Formula, '[A-Z][a-z]?')) %>%
  mutate(elements = map(elements, function(x) { paste(unique(unlist(x)), collapse=';')  })) %>%
  mutate(elements = str_replace(elements, 'NA', NA_character_)) %>%
  mutate(Formula = ifelse(!is.na(Formula), str_replace_all(Formula, '\\_(.*?)\\_',"<sub>\\1</sub>"), NA)) %>%
  mutate(Formula = ifelse(!is.na(Formula), str_replace_all(Formula, '\\^(.*?)\\^',"<sup>\\1</sup>"), NA)) %>%
  mutate(silicate_html = ifelse(!is.na(silicates_real), str_replace_all(silicates_real, '\\_(.*?)\\_',"<sub>\\1</sub>"), NA)) %>%
  mutate(silicate_html = ifelse(!is.na(silicate_html), str_replace_all(silicate_html, '\\^(.*?)\\^',"<sup>\\1</sup>"), NA)) %>%
  mutate(cation_html = ifelse(!is.na(cations_real), str_replace_all(cations_real, '\\_(.*?)\\_',"<sub>\\1</sub>"), NA)) %>%
  mutate(cation_html = ifelse(!is.na(cation_html), str_replace_all(cation_html, '\\^(.*?)\\^',"<sup>\\1</sup>"), NA)) %>%
  mutate(anion_html = ifelse(!is.na(anions_real), str_replace_all(anions_real, '\\_(.*?)\\_',"<sub>\\1</sub>"), NA)) %>%
  mutate(anion_html = ifelse(!is.na(anion_html), str_replace_all(anion_html, '\\^(.*?)\\^',"<sup>\\1</sup>"), NA)) %>%
  mutate(other_html = ifelse(!is.na(other_real), str_replace_all(other_real, '\\_(.*?)\\_',"<sub>\\1</sub>"), NA)) %>%
  mutate(other_html = ifelse(!is.na(other_html), str_replace_all(other_html, '\\^(.*?)\\^',"<sup>\\1</sup>"), NA)) %>%
  left_join(names, by='Mineral_Name') %>%
  left_join(status_data, by='Mineral_Name') %>%
  rename(strunz=Strunz, formula=Formula, ima=IMA.Status, cation=cations_theoretical, anion=anions_theoretical,silicate=silicates_theoretical,
         year=discovery_year_min,other=other_theoretical, name=Mineral_Name) %>%
  filter(str_detect(all_indexes, '0.0')) %>%
  select(name,
         cation,
         anion,
         silicate,
         silicate_html,
         cation_html,
         anion_html,
         elements,
         strunz,
         formula,
         ima,
         year)
  
periodic_table %>% 
  filter(Mineral_Name == 'Alumino-oxy-rossmanite') %>%
  select(elements)

# Export data
write.csv(periodic_table, 'periodic_table_10022021.csv', na='', quote = F, row.names = F)
