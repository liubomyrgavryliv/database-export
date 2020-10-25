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
output <- tibble('mineral_name'=NA,'ion_position'=NA, 'ion'=NA)
gr_ions <- data[1:10,] %>%
  select(name, A,B,C,D,E,F,X1,X2,X3,Y1,Y2,Y3,V,W,Z) %>%
  mutate(mineral_name=name) %>%
  nest(data = c(mineral_name, A, B, C, D, E, F, X1, X2, X3, Y1, Y2, Y3, V, W, Z)) %>%
  rowwise() %>%
  mutate_at('data', .funs=function(x){
    local_data <- x %>% select(where(~ !(all(is.na(.)))))
    output <<- output %>% 
      add_row('mineral_name'=x$mineral_name,'ion_position'='A', 'ion'=x$A)
    return(x)
    # if (!is.na(x$A)) rbind(c("A",x$A))
    # if (!is.na(x$B)) rbind(c("B",x$B))
    # if (!is.na(x$C)) rbind(c("C",x$C))
    # if (!is.na(x$D)) rbind(c("D",x$D))
    # if (!is.na(x$E)) rbind(c("E",x$E))
    
  })

gr_ions$data[2][[1]] %>% select(where(~ !(all(is.na(.)))))

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ms_species_ions;")
dbWriteTable(conn, "ms_species_ions", ms_species_ions, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_positions_list, file = paste0(path, 'io_positions_list.csv'), na='')







