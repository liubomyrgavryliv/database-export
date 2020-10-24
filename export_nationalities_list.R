library(tidyverse)
library(googlesheets4)
library(DBI)
library(RCurl)
setwd("~/post-doc/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


# LOAD DATA -------------------------------------------------------------------
x <- getURL("https://gist.githubusercontent.com/marijn/274449/raw/0045fb5f54f9ad357e301cf30e23d9834058618a/nationalities.txt")
nationalities_list <- read.delim(text = x, header = F) %>%
  rename(nationality = V1)

# EXPORT ms_species ------------------------------------------------------------
write_csv(nationalities_list, path = paste0(path, 'nationalities_list.csv'), na='')
