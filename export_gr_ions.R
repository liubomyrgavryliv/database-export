library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

io_positions_list = tbl(conn, 'io_positions_list')
ms_species = tbl(conn, 'ms_species')
ions_list = tbl(conn, 'ions_list')
io_types_list = tbl(conn, 'io_types_list')

# create subsets for different ions types 
ions_list <- ions_list %>%
  inner_join(io_types_list, copy=TRUE)
  
anions <- ions_list %>%
  filter(ion_type_name == 'Anion')
cations <- ions_list %>%
  filter(ion_type_name == 'Cation')
silicates <- ions_list %>%
  filter(ion_type_name == 'Silicate')
others <- ions_list %>%
  filter(ion_type_name == 'Other')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'Groups_ver1',
  range = 'A:AA',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# parse hierarchy levels -------------------------------------------------------
data <- initial %>% 
  select(!c(status_id, relation_name, chemical_label, structural_label)) %>%
  filter(is.na(group) | !str_detect(group, '^\"'))

# turn hierarchy to one level -------------------------------------------------
supergroups <- data %>%
  filter(!is.na(supergroup) & is.na(group) & is.na(subgroup) & is.na(root) & is.na(serie) & is.na(mineral_name)) %>%
  select(!c(group, subgroup, root, serie, mineral_name)) %>%
  rename('name' = 'supergroup') %>%
  mutate(type = paste0('supergroup'))

groups <- data %>%
  filter(!is.na(group) & is.na(subgroup) & is.na(root) & is.na(serie) & is.na(mineral_name)) %>%
  select(!c(supergroup, subgroup, root, serie, mineral_name)) %>%
  rename('name' = 'group') %>%
  mutate(type = paste0('group'))

subgroups <- data %>%
  filter(!is.na(subgroup) & is.na(root) & is.na(serie) & is.na(mineral_name)) %>%
  select(!c(supergroup, group, root, serie, mineral_name)) %>%
  rename('name' = 'subgroup') %>%
  mutate(type = paste0('subgroup'))

roots <- data %>%
  filter(!is.na(root) & is.na(serie) & is.na(mineral_name)) %>%
  select(!c(supergroup, group, subgroup, serie, mineral_name)) %>%
  rename('name' = 'root') %>%
  mutate(type = paste0('root'))

series <- data %>%
  filter(!is.na(serie) & is.na(mineral_name)) %>%
  select(!c(supergroup, group, root, subgroup, mineral_name)) %>%
  rename('name' = 'serie') %>%
  mutate(type = paste0('serie'))

minerals <- data %>%
  filter(!is.na(mineral_name)) %>%
  select(!c(supergroup, group, root, subgroup, serie)) %>%
  rename('name' = 'mineral_name') %>%
  mutate(type = paste0('mineral'))

data <- 
  bind_rows(supergroups, groups, subgroups, roots, series, minerals) %>%
  distinct(name, .keep_all = TRUE)

rm(list = c('supergroups', 'groups', 'subgroups', 'roots', 'series', 'minerals'))

# merge all ions ----------------------------------------------------------------
output <- tibble('mineral_name'=NA,'ion_position_name'=NA, 'ion'=NA)
data %>%
  select(name, A,B,C,D,E,F,X1,X2,X3,Y1,Y2,Y3,V,W,Z) %>%
  mutate(mineral_name=name) %>%
  nest(data = c(mineral_name, A, B, C, D, E, F, X1, X2, X3, Y1, Y2, Y3, V, W, Z)) %>%
  rowwise() %>%
  mutate_at('data', .funs=function(x){
    local_data <- x %>% select(where(~ !(all(is.na(.)))))
    for (col in colnames(local_data)[-1]) {
      output <<- output %>% 
        add_row('mineral_name'=local_data$mineral_name,'ion_position_name'=col, 'ion'=local_data[[col]])
    }
  })

cations_subset <- output[-1,] %>%
  mutate(ion = str_split(ion, ';')) %>%
  unchop(ion, keep_empty = FALSE) %>%
  separate(ion, c('ion','ion_quantity'), sep=' x ', remove=FALSE) %>%
  filter(str_detect(ion_position_name, 'A|B|C|D|E|F|X1|X2|X3')) %>%
  left_join(cations, by=c('ion'='formula'), copy=TRUE) %>%
  left_join(others, by=c('ion'='formula'), copy=TRUE) %>%
  mutate(ion_id = ifelse(is.na(ion_id.x), ion_id.y, ion_id.x)) %>%
  select(mineral_name, ion_position_name, ion_id, ion_quantity) %>%
  filter(!is.na(ion_id)) # self check - check if all matched!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

anions_subset <- output[-1,] %>%
  mutate(ion = str_split(ion, ';')) %>%
  unchop(ion, keep_empty = FALSE) %>%
  separate(ion, c('ion','ion_quantity'), sep=' x ', remove=FALSE) %>%
  mutate(ion = str_split(ion, ' or ')) %>% # filter(mineral_name == 'Hydrotalcite Group')
  unchop(ion, keep_empty = FALSE) %>%
  filter(str_detect(ion_position_name, 'Y1|Y2|Y3|V|W|Z')) %>%
  left_join(anions, by=c('ion'='formula'), copy=TRUE) %>%
  left_join(cations, by=c('ion'='formula'), copy=TRUE) %>%
  mutate(ion_id = ifelse(is.na(ion_id.x), ion_id.y, ion_id.x)) %>%
  select(mineral_name, ion_position_name, ion, ion_id, ion_quantity) %>%
  left_join(silicates, by=c('ion'='formula'), copy=TRUE) %>%
  mutate(ion_id = ifelse(is.na(ion_id.x), ion_id.y, ion_id.x)) %>%
  select(mineral_name, ion_position_name, ion, ion_id, ion_quantity) %>%
  left_join(others, by=c('ion'='formula'), copy=TRUE) %>%
  mutate(ion_id = ifelse(is.na(ion_id.x), ion_id.y, ion_id.x)) %>%
  select(mineral_name, ion_position_name, ion_id, ion_quantity) %>%
  filter(!is.na(ion_id)) # self check - check if all matched!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

gr_ions <- rbind(cations_subset, anions_subset) %>%
  left_join(io_positions_list, by='ion_position_name', copy=TRUE) %>%
  left_join(ms_species, by='mineral_name', copy=TRUE) %>%
  select(mineral_id, ion_position_id, ion_id, ion_quantity)

check <- gr_ions %>%
  group_by(mineral_name, ion_position_name, ion_id, ion_quantity) %>%
  filter(n() > 1)
  

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM gr_ions;")
dbWriteTable(conn, "gr_ions", gr_ions, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_positions_list, file = paste0(path, 'io_positions_list.csv'), na='')







