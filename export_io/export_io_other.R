library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')


io_class = tbl(conn, 'io_class')
io_subclass = tbl(conn, 'io_subclass')
io_group = tbl(conn, 'io_group')
io_subgroup = tbl(conn, 'io_subgroup')

#Load data ---------------------------------------------------------------------
other <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Neutral, organic and other compounds',
  range = 'A:M',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

master <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:M',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Create other_theoretical column for masterlist ----------------------------------
other_in_cations <- master %>%
  select(Mineral_Name, cations_theoretical, other_theoretical) %>%
  mutate(cations_theoretical = str_split(cations_theoretical, ';')) %>%
  unchop(cations_theoretical, keep_empty = TRUE) %>%
  mutate(other_theoretical = str_split(other_theoretical, ';')) %>%
  unchop(other_theoretical, keep_empty = TRUE) %>%
  left_join(other, by=c('cations_theoretical' = 'Ion'), copy=TRUE) %>%
  mutate(other_theoretical_to_add = ifelse(is.na(`Name (long name)`), NA, cations_theoretical),
         cations_theoretical = ifelse(is.na(`Name (long name)`), cations_theoretical, NA)) %>%
  unite('other_theoretical', c('other_theoretical', 'other_theoretical_to_add'), na.rm=TRUE, sep=';') %>%
  select(Mineral_Name, cations_theoretical, other_theoretical) %>%
  na_if('') %>%
  mutate(other_theoretical = str_split(other_theoretical, ';')) %>%
  unchop(other_theoretical, keep_empty = TRUE) %>%
  group_by(Mineral_Name) %>%
  summarise(cations_theoretical = paste(unique(na.omit(cations_theoretical)), collapse=';'),
            other_theoretical = paste(unique(na.omit(other_theoretical)), collapse=';')) %>%
  na_if('')

# Parse other ----------------------------------------------------------------

other_ids <- other %>%
  select(Ion) %>%
  mutate(id = paste0(1:nrow(other)))

io_others <- other %>%
  select(`Name (long name)`, Ion, `Ion (oxidation states)`, Class, Subclass, `Structure description`, Geometry) %>%
  left_join(io_class, by=c('Class'='class_name'), copy=T) %>%
  left_join(io_subclass, by=c('Subclass'='subclass_name'), copy=T) %>%
  select(`Name (long name)`, Ion,`Ion (oxidation states)`, class_id, subclass_id, `Structure description`, Geometry) %>%
  rename(other_name=`Name (long name)`, formula=Ion, formula_with_oxidation=`Ion (oxidation states)`,
         structure_description=`Structure description`,
         geometry=Geometry) %>%
  mutate(other_id = other_ids$id) %>%
  select(other_id, other_name, formula, formula_with_oxidation, class_id, subclass_id,
         structure_description, geometry) %>%
  distinct(formula, .keep_all = TRUE)

# find duplicates -------------------------------------------------------------
io_anions %>%
  group_by(formula) %>%
  filter(n()>1)


# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_others;")
dbWriteTable(conn, "io_others", io_others, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(export, path = paste0(path, 'export_other_theoretical.csv'), na='')
