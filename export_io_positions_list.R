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


#Create data ---------------------------------------------------------------------
io_positions_list<- tibble(
              position_name = c('A','B','C','D','E','F','X1','X2','X3','Y1','Y2','Y3','V','W','Z')
            )

# UPLOAD DATA TO DB
dbSendQuery(conn, "DELETE FROM io_positions_list;")
dbWriteTable(conn, "io_positions_list", io_positions_list, append=TRUE)

dbDisconnect(conn)

# EXPORT ms_species ------------------------------------------------------------
write_csv(io_positions_list, file = paste0(path, 'io_positions_list.csv'), na='')







