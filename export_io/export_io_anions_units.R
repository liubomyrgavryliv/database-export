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
io_anions_units <- initial %>%
  select(Ion, `Cation Subunites`,`Anion Subunites`,`Silicate Subunites`) %>%
  rename(formula=Ion, anion_subunit=`Anion Subunites`, cation_subunit=`Cation Subunites`, 
         silicate_subunit=`Silicate Subunites`) %>%
  mutate(anion_subunit = str_split(anion_subunit, ';|; +'),
         cation_subunit = str_split(cation_subunit, ';|; +'),
         silicate_subunit = str_split(silicate_subunit, ';|; +')) %>%
  unchop(anion_subunit, keep_empty = T) %>%
  unchop(cation_subunit, keep_empty = T) %>%
  unchop(silicate_subunit, keep_empty = T) %>%
  filter(!is.na(anion_subunit) | !is.na(cation_subunit) | !is.na(silicate_subunit)) #%>%
  # left_join(io_anions, by=c('formula'='formula'), copy=T) %>%
  # select(anion_id, anion_subunit, cation_subunit, silicate_subunit)
  # ... join io_cations and io_silicates

# CHECK ALL SUBUNITS IF PRESENT ! ----------------------------------------------
cations  <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Cations',
  range = 'A:J',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

check_anion_subunit <- io_anions_units %>%
  left_join(io_anions, by=c('anion_subunit'='formula'), copy=TRUE) %>%
  filter(!is.na(anion_subunit) & is.na(anion_id)) %>%
  select(formula, anion_subunit, cation_subunit, silicate_subunit)

check_cation_subunit <- io_anions_units %>%
  left_join(cations, by=c('cation_subunit'='Ion'), copy=TRUE) %>%
  filter(!is.na(cation_subunit) & is.na(`Name (long name)`)) %>%
  select(formula, anion_subunit, cation_subunit, silicate_subunit)

check <- union(check_anion_subunit, check_cation_subunit) %>%
  group_by(formula) %>%
  summarise(anion_subunit=paste(unique(na.omit(anion_subunit)), collapse=';'),
            cation_subunit=paste(unique(na.omit(cation_subunit)), collapse=';'),
            silicate_subunit=paste(unique(na.omit(silicate_subunit)), collapse=';')) %>%
  na_if('')

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_anions;")
dbWriteTable(conn, "io_anions", io_anions, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(anions_class, path = paste0(path, 'anions_class.csv'), na='')
write_csv(check, path = paste0(path, 'io_anions_units_check.csv'), na='')







