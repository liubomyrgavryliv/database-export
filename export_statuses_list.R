library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'

# LOAD data ---------------------------------------------------------------------
statuses_list <- googlesheets4::read_sheet(
  ss='15H14KCzfH8c7vkPkoUmdaUeS5zNg877lgdFVLw3o6bs',
  sheet = 'statuses_txt',
  range = 'A:C',
  col_names = TRUE,
  na = ""
)

# PROCESS data -----------------------------------------------------------------
  


# EXPORT data ------------------------------------------------------------------
write_csv(statuses_list, path = paste0(path, 'statuses_list.csv'), na='')
