library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  #range = 'A:J',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

ions <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Ions',
  range = 'A:E',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 



# LOAD DATA -------------------------------------------------------------------

# CREATE DATA TO PROCESS FOR VITALII - OMIT THIS STEP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ns_anions_unique <- initial %>%
  select(anions_theoretical) %>%
  mutate(anions_theoretical = str_split(anions_theoretical, ';')) %>%
  unchop(anions_theoretical, keep_empty = TRUE) %>%
  mutate(anions_theoretical = str_replace(anions_theoretical, '^\\((?!.*\\)[^\\=].)', '')) %>%
  mutate(anions_theoretical = str_replace(anions_theoretical, '=.*', '')) %>%
  mutate(anions_theoretical = str_replace(anions_theoretical, '\\)$', '')) %>%
  mutate(anions_theoretical = str_split(anions_theoretical, ' \\+ ')) %>%
  unchop(anions_theoretical, keep_empty = TRUE) %>%
  filter(!is.na(anions_theoretical)) %>%
  distinct(anions_theoretical) %>%
  arrange(anions_theoretical)

# compare "theoretical" anions and "real"
ns_anions_all <- initial %>%
  select(Mineral_Name, Formula, anions_theoretical, anions_real) %>%
  mutate(anions_theoretical = str_split(anions_theoretical, ';')) %>%
  unchop(anions_theoretical, keep_empty = TRUE) %>% #parse anions_theoretical level
  mutate(anions_theoretical = str_replace(anions_theoretical, '^\\((?!.*\\)[^\\=].)', '')) %>%
  mutate(anions_theoretical = str_replace(anions_theoretical, '=.*', '')) %>%
  mutate(anions_theoretical = str_replace(anions_theoretical, '\\)$', '')) %>%
  mutate(anions_theoretical = str_split(anions_theoretical, ' \\+ ')) %>%
  unchop(anions_theoretical, keep_empty = TRUE) %>%
  mutate(anions_real = str_split(anions_real, ';')) %>% #parse anions_real level
  unchop(anions_real, keep_empty = TRUE) %>% 
  mutate(anions_real = str_replace(anions_real, ' x.*', '')) %>%
  mutate(anions_real = str_replace(anions_real, '^\\((?!.*\\)[^\\=].)', '')) %>%
  mutate(anions_real = str_replace(anions_real, '^\\((?=.*\\)$)', '')) %>%
  mutate(anions_real = str_replace(anions_real, '\\=.*', '')) %>%
  mutate(anions_real = str_replace(anions_real, '\\)$', '')) %>%
  mutate(anions_real = str_split(anions_real, ' \\+ ')) %>%
  unchop(anions_real, keep_empty = TRUE) %>%        
  mutate(anion_el = str_extract(anions_theoretical, '[A-Z][a-z]?')) %>%
  mutate(anions_real = ifelse(str_detect(anions_real, anion_el), anions_real, '')) %>%
  filter(!is.na(anions_theoretical)) %>%
  group_by(anions_theoretical) %>%
  summarise(anions_real = paste(unique(anions_real), collapse=';'),
            anions_theoretical = anions_theoretical) %>%
  mutate(anions_real = str_split(anions_real, ';')) %>%
  unchop(anions_real, keep_empty = FALSE) %>%
  filter(anions_real != '' & anions_theoretical != '') %>%
  distinct() %>%
  arrange(anions_theoretical)

# create unique ions list from cations-silicates-anions
ns_ions_unique <- initial %>%
  select(anions_theoretical) %>%
  mutate(anions_theoretical = str_split(anions_theoretical, ';')) %>%
  unchop(anions_theoretical, keep_empty = TRUE) %>%
  distinct() %>%
  arrange(anions_theoretical)

# compare unique 'theoretical' anions with those stored in ions_table
anions_absent <-
  ns_anions_unique %>%
  anti_join(ions, by = c('anions_theoretical' = 'Ion'))

ions_duplicates <- ions %>% 
       group_by(Ion, Type) %>% 
       filter(n()>1)

# filter ions present in ions_database but absent in ns list

io_absent <- ions %>%
  anti_join(ns_ions_unique, by = c('Ion'='ions'))
  
# EXPORT DATA ------------------------------------------------------------------
write_csv(ns_anions_unique, path = paste0(path, 'ns_anions_unique.csv'), na='')
write_csv(anions_absent, path = paste0(path, 'anions_absent.csv'), na='')
write_csv(ions_duplicates, path = paste0(path, 'ions_duplicates.csv'), na='')
write_csv(io_absent, path = paste0(path, 'io_absent.csv'), na='')

