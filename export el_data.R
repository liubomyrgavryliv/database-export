library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')


#Load data ---------------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Elements_database',
  range = 'A:AV',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# PROCESS DATA -----------------------------------------------------------------
el_data <- initial %>%
  mutate(atomic_mass = str_replace(atomic_mass, ',', '.'),
         electronegativity = str_replace(electronegativity, ',', '.'),
         ion_radius = str_replace(ion_radius, ',', '.'),
         density = str_replace(density, ',', '.'),
         crust_crc_handbook = str_replace(crust_crc_handbook, ',', '.'),
         crust_kaye_laby = str_replace(crust_kaye_laby, ',', '.'),
         crust_greenwood = str_replace(crust_greenwood, ',', '.'),
         crust_ahrens_taylor = str_replace(crust_ahrens_taylor, ',', '.'),
         crust_ahrens_wanke = str_replace(crust_ahrens_wanke, ',', '.'),
         crust_ahrens_waver = str_replace(crust_ahrens_waver, ',', '.'),
         upper_crust_ahrens_taylor = str_replace(upper_crust_ahrens_taylor, ',', '.'),
         upper_crust_ahrens_shaw = str_replace(upper_crust_ahrens_shaw, ',', '.'),
         sea_water_crc_handbook = str_replace(sea_water_crc_handbook, ',', '.'),
         sea_water_kaye_laby = str_replace(sea_water_kaye_laby, ',', '.'),
         sun_kaye_laby = str_replace(sun_kaye_laby, ',', '.'),
         solar_system_kaye_laby = str_replace(solar_system_kaye_laby, ',', '.'),
         solar_system_ahrens = str_replace(solar_system_ahrens, ',', '.'),
         solar_system_ahrens_with_uncertainty = str_replace(solar_system_ahrens_with_uncertainty, ',', '.')
  ) %>%
  arrange(!desc(atomic_number))

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM el_data;")
dbWriteTable(conn, "el_data", el_data, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(el_data, path = paste0(path, 'el_data.csv'), na='')
