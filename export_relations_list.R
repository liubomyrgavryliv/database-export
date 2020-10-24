library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/post-doc/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'master', 
                  host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                  port = 5433,
                  user = 'postgres',
                  password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')
# LOAD data ---------------------------------------------------------------------
relations_list <- tribble(
                      ~type, ~note,
                      "mineralogical", 'varieties, synonyms or other references which have mineralogical/chemical meaning',
                      "naming", 'references without mineralogical meaning, eg lexical'
                    )

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM relations_list WHERE 1=1;")
dbAppendTable(conn, "relations_list", relations_list)
dbDisconnect(conn)

# EXPORT data
write_csv(relations_list, path = paste0(path, 'relations_list.csv'), na='')

