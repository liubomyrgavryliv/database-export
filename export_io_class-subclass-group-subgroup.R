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


anions_class = tbl(conn, 'anions_class')
anions_subclass = tbl(conn, 'anions_subclass')
anions_group = tbl(conn, 'anions_group')
anions_subgroup = tbl(conn, 'anions_subgroup')
#Load data ---------------------------------------------------------------------

anions <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Anions',
  range = 'A:E',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) %>%
  select(Class, Subclass, Group, Subgroup)

cations <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Cations',
  range = 'A:N',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) %>%
  select(Class, Subclass) %>%
  mutate(Group = NA,
         Subgroup = NA)

neutral <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Neutral, organic and other compounds',
  range = 'A:K',
  col_names = TRUE,
  col_types = 'c',
  na = ""
)  %>%
  select(Class, Subclass) %>%
  mutate(Group = NA,
         Subgroup = NA)

silicates <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Silicates',
  range = 'A:K',
  col_names = TRUE,
  col_types = 'c',
  na = ""
)  %>%
  select(Class, Subclass) %>%
  mutate(Group = NA,
         Subgroup = NA)

initial <- rbind(anions, cations, silicates, neutral)
  
# Parse classes ----------------------------------------------------------------
io_class <- initial %>%
  select(Class) %>%
  rename(class_name=Class) %>%
  distinct(class_name) %>%
  arrange(class_name)

io_subclass <- initial %>%
  select(Subclass) %>%
  rename(subclass_name=Subclass) %>%
  distinct(subclass_name) %>%
  filter(!is.na(subclass_name)) %>%
  arrange(subclass_name)

io_group <- initial %>%
  select(Group) %>%
  rename(group_name=Group) %>%
  distinct(group_name) %>%
  filter(!is.na(group_name)) %>%
  arrange(group_name)

io_subgroup <- initial %>%
  select(Subgroup) %>%
  rename(subgroup_name=Subgroup) %>%
  distinct(subgroup_name) %>%
  filter(!is.na(subgroup_name)) %>%
  arrange(subgroup_name)
  
# an_hierarchy <- initial %>%
#   select(Class, Subclass, Group, Subgroup) %>%
#   rename(class=Class, subclass=Subclass, group=Group, subgroup=Subgroup) %>%
#   distinct() %>%
#   arrange(class, subclass, group, subgroup) %>%
#   left_join(anions_class, by=c('class'='class_name'), copy=TRUE) %>%
#   left_join(anions_subclass, by=c('subclass'='subclass_name'), copy=TRUE) %>%
#   left_join(anions_group, by=c('group'='group_name'), copy=TRUE) %>%
#   left_join(anions_subgroup, by=c('subgroup'='subgroup_name'), copy=TRUE) %>%
#   select(class_id, subclass_id, group_id, subgroup_id)

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_class;")
dbWriteTable(conn, "io_class", io_class, append=TRUE)

dbSendQuery(conn, "DELETE FROM io_subclass;")
dbWriteTable(conn, "io_subclass", io_subclass, append=TRUE)

dbSendQuery(conn, "DELETE FROM io_group;")
dbWriteTable(conn, "io_group", io_group, append=TRUE)

dbSendQuery(conn, "DELETE FROM io_subgroup;")
dbWriteTable(conn, "io_subgroup", io_subgroup, append=TRUE)
dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_class, path = paste0(path, 'io_class.csv'), na='')
write_csv(io_subclass, path = paste0(path, 'io_subclass.csv'), na='')
write_csv(io_group, path = paste0(path, 'io_group.csv'), na='')
write_csv(io_subgroup, path = paste0(path, 'io_subgroup.csv'), na='')
