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


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  range = 'A:K',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

silicates_unique <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Silicates',
  range = 'A:B',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 
  

# CREATE DATA TO PROCESS FOR VITALII - OMIT THIS STEP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# parse silicates_theoretical

ns_silicates_theoretical <- 
  initial %>%
  select(Mineral_Name, silicates_theoretical) %>%
  mutate(silicates_theoretical = str_split(silicates_theoretical, ';')) %>%
  unchop(silicates_theoretical, keep_empty = TRUE) %>% #parse anions_theoretical level
  distinct(silicates_theoretical) %>%
  arrange(silicates_theoretical)

# silicates, present in Ions_database but absent in NS list
silicates_absent <-
  silicates_unique %>%
  anti_join(ns_silicates_theoretical, by = c('Ion' = 'silicates_theoretical'))

# silicates, present in Ions_database but absent in NS list
silicates_absent <-
  ns_silicates_theoretical %>%
  anti_join(silicates_unique, by = c('silicates_theoretical' = 'Ion'))

ions_duplicates <- silicates_unique %>% 
  group_by(Ion) %>% 
  filter(n()>1)


# UPLOAD DATA TO DB
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(silicates_absent, path = paste0(path, 'silicates_absent.csv'), na='')
