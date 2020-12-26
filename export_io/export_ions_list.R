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


ion_class_list = tbl(conn, 'ion_class_list')
ion_subclass_list = tbl(conn, 'ion_subclass_list')
ion_group_list = tbl(conn, 'ion_group_list')
ion_subgroup_list = tbl(conn, 'ion_subgroup_list')
ion_type_list = tbl(conn, 'ion_type_list')

#Load data ---------------------------------------------------------------------
cations <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Cations',
  range = 'A:O',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

anions <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Anions',
  range = 'A:O',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

other <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Neutral, organic and other compounds',
  range = 'A:M',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 


silicates <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Silicates',
  range = 'A:N',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

# Parse and merge into one table
anions <- anions %>%
  left_join(io_class, by=c('Class'='class_name'), copy=T) %>%
  left_join(io_subclass, by=c('Subclass'='subclass_name'), copy=T) %>%
  left_join(io_group, by=c('Group'='group_name'), copy=T) %>%
  left_join(io_subgroup, by=c('Subgroup'='subgroup_name'), copy=T) %>%
  mutate(expressed_as = NA_character_) %>%
  select(`Name (short name)`, Ion, `Ion (oxidation states)`, `Variety of`, expressed_as, class_id, subclass_id, group_id,
         subgroup_id, `Structure description`, Geometry, Type) %>%
  rename(ion_name=`Name (short name)`, formula=Ion, formula_with_oxidation=`Ion (oxidation states)`,
         variety_of=`Variety of`, structure_description=`Structure description`,
         geometry=Geometry, ion_type_name=Type)

cations <- cations %>%
  left_join(io_class, by=c('Class'='class_name'), copy=T) %>%
  left_join(io_subclass, by=c('Subclass'='subclass_name'), copy=T) %>%
  mutate(variety_of = NA_character_,
         group_id=NA_character_,
         subgroup_id=NA_character_) %>%
  select(`Name (long name)`, Ion, `Ion (oxidation states)`,variety_of, `Can be expressed as`, class_id, subclass_id, group_id, 
         subgroup_id, `Structure description`, Geometry, Type) %>%
  rename(ion_name=`Name (long name)`, formula=Ion, formula_with_oxidation=`Ion (oxidation states)`,expressed_as=`Can be expressed as`,
         structure_description=`Structure description`,
         geometry=Geometry, ion_type_name=Type)

silicates <- silicates %>%
  left_join(io_class, by=c('Class'='class_name'), copy=T) %>%
  left_join(io_subclass, by=c('Subclass'='subclass_name'), copy=T) %>%
  left_join(io_subgroup, by=c('Subgroup'='subgroup_name'), copy=T) %>%
  mutate(group_id=NA_character_, expressed_as=NA_character_, geometry=NA_character_,
         ion_type_name='Silicate') %>%                                                 # OMIT THIS AFTER RESOLVE ----------------------------------------
  select(`Name (short name)`, Ion, `Ion (oxidation states)`, `Variety of`, expressed_as, class_id, subclass_id, group_id, 
         subgroup_id, `Name (long name)`, geometry, ion_type_name) %>%
  rename(ion_name=`Name (short name)`, formula=Ion, formula_with_oxidation=`Ion (oxidation states)`, variety_of=`Variety of`,
         structure_description=`Name (long name)`)

other <- other %>%
  left_join(io_class, by=c('Class'='class_name'), copy=T) %>%
  left_join(io_subclass, by=c('Subclass'='subclass_name'), copy=T) %>%
  mutate(group_id=NA_character_,subgroup_id=NA_character_, expressed_as=NA_character_, variety_of=NA_character_) %>%
  select(`Name (long name)`, Ion, `Ion (oxidation states)`, variety_of, expressed_as, class_id, subclass_id, group_id, 
       subgroup_id, `Structure description`, Geometry, Type) %>%
  rename(ion_name=`Name (long name)`, formula=Ion, formula_with_oxidation=`Ion (oxidation states)`,
         structure_description=`Structure description`, geometry=Geometry, ion_type_name=Type)

ions = rbind(cations, anions, silicates, other) %>%
  arrange(ion_type_name, ion_name) %>%
  left_join(io_types_list, by=c('ion_type_name'='ion_type_name'), copy=TRUE) %>%
  filter(!is.na(ion_name)) %>% # OMIT AFTER RESOLVE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  distinct(ion_type_id, formula, .keep_all = TRUE) %>% # OMIT AFTER VITALII REMOVE DUPLICATE SILICATE !!!!!!!!!!!!!!!!!!!!
  mutate(ion_id=row_number()) %>%
  select(c('ion_id',"ion_type_id","ion_name","formula","formula_with_oxidation","variety_of",
           "expressed_as","class_id","subclass_id","group_id","subgroup_id","structure_description",
           "geometry") )

ions_ids <- ions %>%
  select(formula) %>%
  mutate(variety_id = row_number())
  
ions_list <- ions %>%
  left_join(ions_ids, by=c('variety_of'='formula')) %>%
  mutate(variety_of=variety_id) %>%
  select(!variety_id)
  

# Parse anions ----------------------------------------------------------------

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM ions_list;")
dbWriteTable(conn, "ions_list", ions_list, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(ions_list, path = paste0(path, 'ions_list.csv'), na='')
