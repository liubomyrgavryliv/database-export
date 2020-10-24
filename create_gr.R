library(tidyverse)
library(googlesheets4)
library(DBI)
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL/")
rm(list=ls())
path <- 'export/'
source('functions.R')

# read Groups data ------------------------------------------------------------
initial <- googlesheets4::read_sheet(
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'Groups_formulae_table',
  range = 'B:GB',
  col_names = TRUE,
  col_types = 'c',
  na = ""
)

data <- initial %>% 
  select(!c(Index, Relation_name, Chemical_label, Structural_label)) %>%
  filter(is.na(Group) | !str_detect(Group, '^\"'))

# turn hierarchy to one level -------------------------------------------------
supergroups <- data %>%
  filter(!is.na(Supergroup) & is.na(Group) & is.na(Subgroup) & is.na(Aliases) & is.na(Series) & is.na(Minerals_Names)) %>%
  select(!c(Group, Subgroup, Aliases, Series, Minerals_Names)) %>%
  rename('Name' = 'Supergroup') %>%
  mutate(type = paste0('Supergroup'))

groups <- data %>%
  filter(!is.na(Group) & is.na(Subgroup) & is.na(Aliases) & is.na(Series) & is.na(Minerals_Names)) %>%
  select(!c(Supergroup, Subgroup, Aliases, Series, Minerals_Names)) %>%
  rename('Name' = 'Group') %>%
  mutate(type = paste0('Group'))

subgroups <- data %>%
  filter(!is.na(Subgroup) & is.na(Aliases) & is.na(Series) & is.na(Minerals_Names)) %>%
  select(!c(Supergroup, Group, Aliases, Series, Minerals_Names)) %>%
  rename('Name' = 'Subgroup') %>%
  mutate(type = paste0('Subgroup'))

roots <- data %>%
  filter(!is.na(Aliases) & is.na(Series) & is.na(Minerals_Names)) %>%
  select(!c(Supergroup, Group, Subgroup, Series, Minerals_Names)) %>%
  rename('Name' = 'Aliases') %>%
  mutate(type = paste0('Root'))

series <- data %>%
  filter(!is.na(Series) & is.na(Minerals_Names)) %>%
  select(!c(Supergroup, Group, Aliases, Subgroup, Minerals_Names)) %>%
  rename('Name' = 'Series') %>%
  mutate(type = paste0('Serie'))

minerals <- data %>%
  filter(!is.na(Minerals_Names)) %>%
  select(!c(Supergroup, Group, Aliases, Subgroup, Series)) %>%
  rename('Name' = 'Minerals_Names') %>%
  mutate(type = paste0('Mineral'))

data <- 
  bind_rows(supergroups, groups, subgroups, roots, series, minerals) %>%
  distinct(Name, .keep_all = TRUE)

rm(list = c('supergroups', 'groups', 'subgroups', 'roots', 'series', 'minerals'))

# Create new columns for ions ---------------------------------------------------

output <- data %>%
  merge_ions('A') %>%
  merge_ions('B') %>%
  merge_ions('C') %>%
  merge_ions('D') %>%
  merge_ions('E') %>%
  merge_ions('F') %>%
  merge_ions('X1') %>%
  merge_ions('X2') %>%
  merge_ions('X3') %>%
  merge_ions('Y1') %>%
  merge_ions('Y2') %>%
  merge_ions('Y3') %>%
  merge_ions('V') %>%
  merge_ions('W') %>%
  merge_ions('Z') %>%
  select(Name, type, Formulae, Note, A, B, C, D, E, F, X1, X2, X3, Y1, Y2, Y3, V, W, Z)


# for testing ------------------------------------------------------------------
column_name = 'W'

test <- data %>% 
                select(Name, matches(ifelse(str_detect(column_name, 'X1|X2|X3|Y1|Y2|Y3'), column_name, paste0(column_name,'[0-9]')))) %>%
                mutate_at(vars(!contains(c('Index', 'Charge', 'Name'))), .funs = function(x){ str_split(x, ', ') }) %>%
                unchop(cols = !contains(c('Index', 'Charge', 'Name'))) %>%
                rowwise() %>%
                mutate_at(vars(!contains(c('Index', 'Charge', 'Name'))), .funs = function(x) {
                                          colname = quo_name(enquo(x))
                                          ion = str_extract(colname, '[A-Z][0-9]-?[a-z]?')
                                          output = list(
                                            c_across(contains(ion))
                                          )
                                          cation = ifelse(is.na(output[[1]][1]), '', output[[1]][1])
                                          charge = output[[1]][2]
                                          index = output[[1]][3]
                                          
                                          charge = ifelse(str_detect(cation, '\\^') || is.na(charge) || cation == '[box]', '', paste0('^',charge,'^'))
                                          index = ifelse(index == 1 || is.na(index) || cation == '[box]', '', paste0(' x ', index))
                                          
                                          ions_cell = paste0(cation, charge, index)
                                          
                               return(ifelse(ions_cell == '', NA, ions_cell))
                  
                }) %>%
                select(!contains(c('Index', 'Charge'))) %>%
                unite(col = !!column_name, !contains(c('Name')), na.rm = TRUE, sep = ', ') %>%
                group_by(Name) %>%
                summarise(across(!!column_name, function(x) {
                          ions = unlist(unique(str_split(x, ', ')))
                          return(paste0(ions, collapse = ', '))
                })) %>%
                distinct(Name, .keep_all = TRUE) %>%
                mutate_all(list(~na_if(.,""))) %>%
                ungroup() %>%
                select(Name, !!column_name)
                  
                            
                



# output ----------------------------------------------------------------------
googlesheets4::write_sheet(output, 
                           ss = '1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
                           sheet = 'GROUPS_psql')
