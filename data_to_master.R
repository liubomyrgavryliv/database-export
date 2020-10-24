library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/post-doc/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'


conn <- dbConnect(RPostgres::Postgres(),dbname = 'masterclone', 
                 host = 'ec2-18-184-252-245.eu-central-1.compute.amazonaws.com',
                 port = 5433,
                 user = 'postgres',
                 password = 'BQBANe++XrmO5xWA3UqipNACx3Mf95kN')

conn <- dbConnect(RPostgres::Postgres(),dbname = 'postgres', 
                  host = 'localhost',
                  port = 5432,
                  user = 'postgres',
                  password = 'mybullets1001')

dbListTables(conn)
test <- dbReadTable(conn, "gr_supergroups")
dbGetQuery(conn, "SELECT * FROM gr_supergroups WHERE supergroup_name ~* 'garnet';")
dbGetQuery(conn, "DELETE from gr_supergroups;")

dbWriteTable(
  conn,
  "gr_supergroups",
  supergroups,
  row.names = FALSE,
  overwrite = FALSE,
  append = TRUE,
  temporary = FALSE,
  copy = TRUE
)


dbDisconnect(conn)
