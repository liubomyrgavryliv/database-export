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
  sheet = 'Groups_formulae_table',
  range = 'B:GB',
  col_names = TRUE,
  col_types = 'c',
  na = ""
)

# concat ion columns with charges
data <- initial %>%
  select(!c('Supergroup','Group','Subgroup','Aliases','Series','Chemical_label', 
            'Structural_label','Minerals_Names','Note','Index','Relation_name','Formulae')) %>%
  mutate(Id = 1:nrow(initial)) %>%
  plain_ions('A') %>%
  plain_ions('B') %>%
  plain_ions('C') %>%
  plain_ions('D') %>%
  plain_ions('E') %>%
  plain_ions('F') %>%
  plain_ions('X1') %>%
  plain_ions('X2') %>%
  plain_ions('X3') %>%
  plain_ions('Y1') %>%
  plain_ions('Y2') %>%
  plain_ions('Y3') %>%
  plain_ions('V') %>%
  plain_ions('W') %>%
  plain_ions('Z') %>%
  select(Id, A, B, C, D, E, F, X1, X2, X3, Y1, Y2, Y3, V, W, Z)
  

  mutate_all(str_split(Cation, ', ')) %>%
  unchop(Cation, keep_empty = FALSE) %>%
  distinct(Cation) %>%
  arrange(Cation)

# check errors -------------------------------
check <- data %>%
  anti_join(ions, by=c('Cation'='Ion'))


# output ----------------------------------------------------------------------
googlesheets4::write_sheet(output, 
                           ss = '1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
                           sheet = 'GROUPS_psql')
