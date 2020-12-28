library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'master.c6ya4cff5frj.eu-central-1.rds.amazonaws.com',
                  port = 5432,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

goldschmidt_class_list = tbl(conn, 'goldschmidt_class_list')
bonding_type_list = tbl(conn, 'bonding_type_list')
chemical_group_list = tbl(conn, 'chemical_group_list')
phase_state_list = tbl(conn, 'phase_state_list')

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
element_list <- initial %>%
  left_join(goldschmidt_class_list, by=c('goldschmidt_classification'='goldschmidt_class_name'), copy=TRUE) %>%
  left_join(bonding_type_list, by=c('bonding_type'='bonding_type_name'), copy=TRUE) %>%
  left_join(chemical_group_list, by=c('group_block'='chemical_group_name'), copy=TRUE) %>%
  left_join(phase_state_list, by=c('standard_state'='phase_state_name'), copy=TRUE) %>%
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
  arrange(!desc(atomic_number)) %>%
  select(element,name,atomic_number,name_alternative,atomic_mass,atomic_mass_standard_uncertainty,electronic_configuration,cpk_hex_color,                       
         goldschmidt_class_id,electronegativity,empirical_atomic_radius,calculated_atomic_radius,            
         van_der_waals_radius,covalent_single_bond_atomic_radius,covalent_triple_bond_atomic_radius,metallic_atomic_radius,              
         ion_radius,ion_radius_charge,ionization_energy,electron_affinity,                   
         oxidation_states,phase_state_id,bonding_type_id,melting_point,                       
         boiling_point,density,chemical_group_id,crust_crc_handbook,                  
         crust_kaye_laby,crust_greenwood,crust_ahrens_taylor,crust_ahrens_wanke,                  
         crust_ahrens_waver,upper_crust_ahrens_taylor,upper_crust_ahrens_shaw,sea_water_crc_handbook,              
         sea_water_kaye_laby,sun_kaye_laby,solar_system_kaye_laby,solar_system_ahrens,                 
         solar_system_ahrens_with_uncertainty,natural_isotopes,name_meaning,discovery_year,                      
         discoverer,application,safety,biological_role)

# UPLOAD DATA TO DB
dbSendQuery(conn, "TRUNCATE TABLE element_list RESTART IDENTITY CASCADE;")
dbWriteTable(conn, "element_list", element_list, append=TRUE)


dbDisconnect(conn)