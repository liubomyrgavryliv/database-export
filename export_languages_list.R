library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


# LOAD DATA -------------------------------------------------------------------
languages_list <- read.csv('export/languages_list_not_parsed.csv') %>%
  rename('639-2' = 'X639.2', "639-3" = "X639.3", '639-5' = "X639.5", "639-1" = "X639.1", 
         "language_name"="Language.name.s..from.ISO.639.2.1.", scope='Scope', type='Type', other_names='Other.name.s.') %>%
  mutate(language_group = NA) %>%
  mutate(language_group = language_name,
         language_name = str_split(language_name, ';')) %>%
  unchop(language_name, keep_empty = TRUE) %>%
  mutate(language_name = str_replace(language_name, '^[^[A-Za-z]]', '')) %>%
  arrange(language_group) %>%
  distinct(language_name, language_group, .keep_all = T) %>%
  select(c("language_name","language_group","other_names","type","scope","639-1", "639-2","639-3","639-5")) 

languages_list[languages_list==""]<-NA



# EXPORT ms_species ------------------------------------------------------------
write_csv(languages_list, path = paste0(path, 'languages_list.csv'), na='')
