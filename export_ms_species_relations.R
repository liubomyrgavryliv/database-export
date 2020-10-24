library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'


conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

ms_species <- tbl(conn, 'ms_species')
# LOAD data ---------------------------------------------------------------------
mn_status <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:N',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, `A synonym of`,`A polytype of`,`A mixture of`,`A variety of`, Mixture_Note)

mn_names_other <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  range = 'A:AL',
  col_names = TRUE,
  na = ""
) %>%
  select(Mineral_Name, name_relation)

# PROCESS data 

mineralogical_relations <- mn_status %>%
  unite('relation_name', 2:5, sep = ', ', na.rm = TRUE) %>%
  mutate(relation_name = str_split(ifelse(str_detect('UM1986-10-CO:ClHMgMnZn (also called Mineral F, Dunn, 1995)',relation_name), NA, relation_name), ', ')) %>%
  unchop(relation_name, keep_empty = TRUE) %>%
  rename(c('name' = 'Mineral_Name'), c('relation_note' = 'Mixture_Note')) %>%
  filter(relation_name != '') %>%
  mutate(relation_type = 1)

mineralogical_relations_mirror <- mineralogical_relations %>%
  rename(relation_name=name, name=relation_name) %>%
  select(name, relation_name, relation_type, relation_note)

mineralogical_relations <- union(mineralogical_relations, mineralogical_relations_mirror)
  
name_relations <- mn_names_other %>%
  mutate(name_relation = ifelse(name_relation == '', NA, name_relation)) %>%
  mutate(name_relation = str_split(name_relation, ';'),
         relation_note = NA,
         relation_type = 2) %>%
  unchop(name_relation, keep_empty = TRUE) %>%
  filter(!is.na(name_relation)) %>%
  rename(name=Mineral_Name, relation_name=name_relation) %>%
  distinct()

name_relations_mirror <- name_relations %>%
  rename(relation_name=name, name=relation_name) %>%
  select(name, relation_name, relation_type, relation_note)

name_relations <- union(name_relations, name_relations_mirror)


ms_species_relations <- union(mineralogical_relations,name_relations) %>%
  distinct() %>%
  arrange(name) %>%
  left_join(ms_species, by=c('name' = 'mineral_name'), copy=TRUE) %>%
  filter(!is.na(mineral_id)) %>% # CHECK IF ALL MINERALS ARE PRESENT IN MASTER TABLE
  select(mineral_id, relation_name, relation_note, relation_type) %>%
  left_join(ms_species, by=c('relation_name' = 'mineral_name'), copy=TRUE) %>%
  select(mineral_id.x, mineral_id.y, relation_note, relation_type) %>%
  rename('mineral_id' = 'mineral_id.x','relation_id' = 'mineral_id.y', relation_type_id=relation_type)
  
  
# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_species_relations WHERE 1=1;")
dbWriteTable(conn, "ms_species_relations", ms_species_relations, append=TRUE)
dbDisconnect(conn)
  
# EXPORT data
write_csv(ms_species_relations, path = paste0(path, 'ms_species_relations.csv'), na='')
