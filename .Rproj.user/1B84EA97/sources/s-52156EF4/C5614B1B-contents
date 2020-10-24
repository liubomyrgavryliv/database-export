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
cations <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Cations',
  range = 'A:O',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# create io_cations ----------------------------------------------------------------

cations_ids <- cations %>%
  select(Ion) %>%
  mutate(id = paste0(1:nrow(cations)))


io_cations <- cations %>%
  select(`Name (long name)`, Ion, `Ion (oxidation states)`, Class, Subclass, `Can be expressed as`, `Structure description`, Geometry) %>%
  left_join(io_class, by=c('Class'='class_name'), copy=T) %>%
  left_join(io_subclass, by=c('Subclass'='subclass_name'), copy=T) %>%
  select(`Name (long name)`, Ion,`Ion (oxidation states)`, class_id, subclass_id, `Can be expressed as`, `Structure description`, Geometry) %>%
  rename(cation_name=`Name (long name)`, formula=Ion, formula_with_oxidation=`Ion (oxidation states)`,
         structure_description=`Structure description`,
         geometry=Geometry, expressed_as=`Can be expressed as`) %>%
  mutate(cation_id = cations_ids$id) %>%
  select(cation_id, cation_name, formula, formula_with_oxidation, class_id, subclass_id,
         structure_description, geometry, expressed_as) %>%
  distinct(formula, .keep_all = TRUE)

# find duplicates -------------------------------------------------------------
io_cations %>%
  group_by(formula) %>%
  filter(n()>1)


# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_cations;")
dbWriteTable(conn, "io_cations", io_cations, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_cations, path = paste0(path, 'io_cations.csv'), na='')
