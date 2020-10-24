library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

# read Groups data ------------------------------------------------------------
cations <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Cations',
  range = 'A:A',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

anions <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Anions',
  range = 'A:A',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

other <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Neutral, organic and other compounds',
  range = 'A:A',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 


silicates <- googlesheets4::read_sheet(
  ss='1FCu3ywv1wVMP6IZjwFezjipXXC74S8hRbLZaNzZCWpg',
  sheet = 'Silicates',
  range = 'A:A',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

ions <- rbind(cations, anions, other, silicates)

initial <- googlesheets4::read_sheet(
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'Groups_ver1',
  range = 'M:AA',
  col_names = TRUE,
  col_types = 'c',
  na = ""
)

# concat ions

cations_groups <- 
  initial %>%
  unite(ions, c(A, B, C, D, E, F, X1, X2, X3, Y1, Y2, Y3, V, W, Z), sep = ', ', na.rm=TRUE) %>%
  mutate(ions=str_split(ions, ', ')) %>%
  unchop(ions, keep_empty = FALSE) %>%
  mutate(ions=str_replace(ions, ' x .*', '')) %>%
  distinct(ions) %>%
  arrange(ions)

# check errors -------------------------------
check <- cations_groups %>%
  anti_join(ions, by=c('ions'='Ion'))

# output ----------------------------------------------------------------------
googlesheets4::write_sheet(data, 
                           ss = '1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
                           sheet = 'Groups_ver1')
