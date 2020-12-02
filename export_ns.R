library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'

# LOAD data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'C:Q',
  col_names = TRUE,
  na = ""
) %>% select(Strunz, CLASS, SUBCLASS, FAMILY)
  
conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

# PROCESS data -----------------------------------------------------------------
ns_class <- initial %>%
  select(Strunz, CLASS) %>%
  mutate(Strunz = str_extract(Strunz, '^[0-9]+')) %>%
  distinct() %>%
  filter(!is.na(CLASS)) %>%
  arrange(Strunz) %>%
  rename(id_class = Strunz, description = CLASS)

ns_subclass <- initial %>%
  select(Strunz, SUBCLASS) %>%
  mutate(Strunz = str_extract(Strunz, '^[0-9]+\\.[A-Z]')) %>%
  separate(col = Strunz, sep = '\\.',into = c('ns_class','ns_subclass')) %>%
  distinct(.keep_all = FALSE) %>%
  arrange(ns_class, ns_subclass) %>%
  filter(!is.na(SUBCLASS)) %>%
  mutate(ns_subclass = paste0(ns_class,'.',ns_subclass)) %>%
  rename(description = SUBCLASS, id_class=ns_class, id_subclass=ns_subclass)
  
ns_family <- initial %>%
  select(Strunz, FAMILY) %>%
  mutate(Strunz = str_extract(Strunz, '^[0-9]+\\.[A-Z][A-Z]')) %>%
  separate(col = Strunz, sep = '\\.',into = c('ns_class','ns_subclass')) %>%
  separate(col = ns_subclass, sep = 1,into = c('ns_subclass','ns_family')) %>%
  distinct(.keep_all = FALSE) %>%
  arrange(ns_class, ns_subclass, ns_family) %>%
  filter(!is.na(FAMILY)) %>%
  mutate(ns_subclass = paste0(ns_class,'.',ns_subclass),
         ns_family = paste0(ns_subclass,ns_family)) %>%
  rename(description = FAMILY, id_class=ns_class, id_subclass=ns_subclass, id_family=ns_family)

# add ns indices with all possible variants: improvement to db schema
to_add_class <- ns_class %>%
  select(id_class, description) %>%
  mutate(id_subclass=NA, 
         id_family=NA)

to_add_subclass <- ns_subclass %>%
  select(id_class, id_subclass, description) %>%
  mutate(id_family=NA)

ns_list<- ns_family %>%
  bind_rows(to_add_subclass) %>%
  bind_rows(to_add_class)

# EXPORT data
write_csv(ns_class, path = paste0(path, 'ns_class.csv'), na='')
write_csv(ns_subclass, path = paste0(path, 'ns_subclass.csv'), na='')
write_csv(ns_family, path = paste0(path, 'ns_family.csv'), na='')
write_csv(ns_list, path = paste0(path, 'ns_list.csv'), na='')
