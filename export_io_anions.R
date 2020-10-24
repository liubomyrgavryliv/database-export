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
io_anions = tbl(conn, 'io_anions')

#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Anions',
  range = 'A:O',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse anions ----------------------------------------------------------------

anions_ids <- initial %>%
  select(Ion) %>%
  mutate(id = paste0(1:nrow(initial)))

io_anions <- initial %>%
  select(`Name (short name)`, Ion, `Ion (oxidation states)`, `Variety of`, Class, Subclass, Group, Subgroup, 
         `Structure description`, Geometry) %>%
  left_join(io_class, by=c('Class'='class_name'), copy=T) %>%
  left_join(io_subclass, by=c('Subclass'='subclass_name'), copy=T) %>%
  left_join(io_group, by=c('Group'='group_name'), copy=T) %>%
  left_join(io_subgroup, by=c('Subgroup'='subgroup_name'), copy=T) %>%
  left_join(anions_ids, by=c(`Variety of`='Ion')) %>%
  select(`Name (short name)`, Ion,`Ion (oxidation states)`, id, class_id, subclass_id, group_id,
         subgroup_id, `Structure description`, Geometry) %>%
  rename(anion_name=`Name (short name)`, formula=Ion, formula_with_oxidation=`Ion (oxidation states)`,
         variety_of=id, structure_description=`Structure description`,
         geometry=Geometry) %>%
  mutate(anion_id = anions_ids$id) %>%
  select(anion_id, anion_name, formula, formula_with_oxidation, variety_of, class_id, subclass_id, group_id, subgroup_id,
         structure_description, geometry) %>%
  distinct(formula, .keep_all = TRUE)

# find duplicates -------------------------------------------------------------
io_anions %>%
  group_by(formula) %>%
  filter(n()>1)
  
# check subunits of anions -----------------------------------------------------


# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_anions;")
dbWriteTable(conn, "io_anions", io_anions, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_anions, path = paste0(path, 'io_anions.csv'), na='')
