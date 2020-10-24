library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


#Load data ---------------------------------------------------------------------
conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

mn_status <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:AA',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, all_indexes, Note) %>%
  rename(note = Note)

groups_formulas <- googlesheets4::read_sheet(
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'GROUPS_psql',
  col_names = TRUE,
  na = ""
)

ns <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:C',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, Strunz) %>%
  rename(Name = Mineral_Name)

all_formulas <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:E',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, Formula)

ms_species_db = tbl(conn, 'ms_species')
# create subsets ---------------------------------------------
supergroups <- mn_status %>%
            filter(str_detect(all_indexes, '1.0')) %>%
            rename(Name = Mineral_Name) %>%
            select(Name) %>%
            groups_ions_to_json(groups_formulas, 'Supergroup')

groups <- mn_status %>%
            filter(str_detect(all_indexes, '1.1')) %>%
            rename(Name = Mineral_Name) %>%
            select(Name) %>%
            groups_ions_to_json(groups_formulas, 'Group')

subgroups <- mn_status %>%
            filter(str_detect(all_indexes, '1.2')) %>%
            rename(Name = Mineral_Name) %>%
            select(Name) %>%
            groups_ions_to_json(groups_formulas, 'Subgroup')

roots <- mn_status %>%
            filter(str_detect(all_indexes, '1.3')) %>%
            rename(Name = Mineral_Name) %>%
            select(Name) %>%
            groups_ions_to_json(groups_formulas, 'Root')

series <- mn_status %>%
            filter(str_detect(all_indexes, '1.4')) %>%
            rename(Name = Mineral_Name) %>%
            select(Name) %>%
            groups_ions_to_json(groups_formulas, 'Serie') %>%
            mutate(Name = str_split(Name, ', ')) %>%
            unchop(Name, keep_empty = TRUE)

minerals <- mn_status %>%
            filter(str_detect(all_indexes, '0.0')) %>%
            rename(Name = Mineral_Name) %>%
            select(Name) %>%
            groups_ions_to_json(groups_formulas, 'Mineral')

# Bind all data, which contain ions and add Formula

ms_species <- 
      bind_rows(supergroups, groups, subgroups, roots, series, minerals) %>%
      full_join(all_formulas, by = c('Name' = 'Mineral_Name')) %>%
      mutate(Formulae = ifelse(is.na(Formulae), Formula, Formulae)) %>%
      select(!Formula) %>%
      rename(Formula = Formulae) %>%
      distinct(Name, .keep_all = TRUE)


# add missing minerals - REMOVE THIS AFTER VITALII ADDS TO NICKEL-STRUNZ PAGE
residual1 <- mn_status %>%
            anti_join(ns, by = c('Mineral_Name' = 'Name')) %>%
            select(Mineral_Name) %>%
            rename(Name = Mineral_Name)
residual2 <- ms_species %>%
            anti_join(ns) %>%
            select(Name)
residual <- residual1 %>%
  bind_rows(residual2) %>%
  mutate(Formula = NA,
         ions = NA,
         Note = NA)
rm(residual1, residual2)

ms_species <- ms_species %>% 
  bind_rows(residual) %>%
  distinct(Name, .keep_all = TRUE)
  

# add ns indices
ms_species <-
  ms_species %>%
  left_join(ns) %>%
  separate(col = 'Strunz', sep = '\\.', into = c('ns_class', 'ns_subclass_family', 'ns_mineral')) %>%
  separate(col = 'ns_subclass_family', sep = 1, into = c('ns_subclass', 'ns_family')) %>%
  mutate(ns_subclass = ifelse(ns_subclass == '0', NA, ns_subclass),
         ns_family = ifelse(ns_family == '0', NA, ns_family)) %>%
  rename(mineral_name=Name,formula=Formula,note=Note)
  
# ADD THE MISSING MINERALS TO DB
ms_species_new <- ms_species %>%
                  anti_join(ms_species_db, by=c('mineral_name'), copy=TRUE)


# upload to DB
dbWriteTable(conn, "ms_species", ms_species_new, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(ms_species, path = paste0(path, 'ms_species.csv'), na='')
