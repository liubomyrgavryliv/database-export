library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
setwd("~/post-doc/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')


# LOAD DATA -------------------------------------------------------------------
x <- getURL("https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv")
countries_list <- read.csv(text = x)

# PROCESS DATA ----------------------------------------------------------------
countries_list <- countries_list %>%
  select(name, alpha.2, alpha.3, country.code, region, sub.region, intermediate.region) %>%
  rename(alpha_2 = alpha.2, alpha_3 = alpha.3, country_code = country.code, sub_region = sub.region, 
         intermediate_region = intermediate.region)

# ADD UNKNOWN HERE--------------------------------------------------------------
countries_list <- countries_list %>%
            add_row(name = 'unknown')

# EXPORT ms_species ------------------------------------------------------------
write_csv(countries_list, path = paste0(path, 'countries_list.csv'), na='')
