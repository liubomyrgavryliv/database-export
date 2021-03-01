library(tidyverse)
library(googlesheets4)
library(tidyjson)
library(DBI)
library(RCurl)
library("writexl")
setwd("~/Dropbox/GP-minerals/R scripts/export_to_SQL")
rm(list=ls())
path <- 'export_e-Rocks/'

#Load data ---------------------------------------------------------------------
e_rocks <- read.csv(paste0(path, 'minerals (10).csv')) %>%
  select(Title, Nid)

status <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  range = 'A:D',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

crystal <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Crystallography',
  range = 'A:G',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

physical <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Physical_properties',
  range = 'A:V',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

status <- 
  status %>%
  filter(str_detect(all_indexes, '0.0')) %>%
  select(Mineral_Name, IMA.Status) %>%
  rename('Approval history' = IMA.Status)

groups <- googlesheets4::read_sheet(
  ss='1Wo6n1xggXkITCCApdt_tLsNHOKMxyOgsMqSVpRecYsE',
  sheet = 'Groups_formulae_table',
  range = 'A:I',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

localities <- googlesheets4::read_sheet(
  ss='19khDKV1CZ6w5cZr3thsDfd68LEY5cv8wIZMwYXL_xkk',
  sheet = 'Locality_count_rruff_2020+Tetiana',
  range = 'A:I',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

names <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Names data',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) 

ns <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Nickel-Strunz',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) %>%
  select(Mineral_Name, Formula, Strunz)

mindex <- googlesheets4::read_sheet(
  ss='19ezVwIShB11MFUokP5K7EEQyLFbRDzwnSNBgf5l7L5k',
  sheet = 'Mindex_parse',
  col_names = TRUE,
  col_types = 'c',
  na = ""
) %>%
  select(`Mineral Name`, Synonyms, Varieties, `Strunz 8th edition`, `Dana 8th edition`, `Hey's 3rd edition`,
         `Geological occurrence`, `Localities`, `URL to e-Rocks`, `References`, `Context`)

  
# Cross-check e-rocks data with ours -------------------------------------



# pre-parse groups ----
groups_parsed <- groups %>%
  select(Minerals_Names, Supergroup, Group, Subgroup, Aliases, Series) %>%
  group_by(Minerals_Names) %>%
  summarise(Supergroup = paste(unique(Supergroup), collapse = ';'),
            Group = paste(unique(Group), collapse = ';'),
            Subgroup = paste(unique(Subgroup), collapse = ';'),
            Aliases = paste(unique(Aliases), collapse = ';'),
            Series = paste(unique(Series), collapse = ';')) %>%
  mutate_all(~str_replace_all(.,'NA', NA_character_))

mindex_out <- status %>%
  left_join(groups_parsed, by = c('Mineral_Name' = 'Minerals_Names')) %>%
  unite('Groups',c('Supergroup', 'Group', 'Subgroup', 'Aliases', 'Series'), sep = ';', remove=F, na.rm=T) %>%
  unite('Groups Short',c('Supergroup', 'Group', 'Series'), sep = ';', remove=T, na.rm=T) %>%
  left_join(crystal) %>%
  select(!Index) %>%
  rename(`Non-standart settings` = Note) %>%
  left_join(physical, by='Mineral_Name') %>%
  select(!c(Index, all_indexes, VarKey)) %>%
  mutate(Hardness = case_when(
    H_min != H_max ~ paste0(H_min, '-', H_max),
    H_min == H_max ~ paste0(H_min)
  ),
        `Density measured` = case_when(
          `D(meas,)_min` != `D(meas,)_max` ~ paste0(`D(meas,)_min`, '-', `D(meas,)_max`),
          `D(meas,)_min` == `D(meas,)_max` ~ paste0(`D(meas,)_min`)
        )) %>%
  left_join(localities, by = c('Mineral_Name' = 'Name')) %>%
  mutate(Index_Legend_Range = str_replace(Index_Legend_Range, '^\\(', '')) %>%
  mutate(Index_Legend_Range = str_replace(Index_Legend_Range, '\\)$', '')) %>%
  left_join(names, by = 'Mineral_Name') %>%
  mutate_all(~str_replace_na(., "")) %>%
  mutate('Type Locality' = ifelse(Type != '', paste0(LOCALITY, ', ', Country, ifelse(Note == '', '', paste0('; ', Note))), NA)) %>%
  mutate(`Named for` = case_when(
    Person != '' ~ paste0(Person, ' (', ifelse(Nationality == '', '', paste0(Nationality, '; ')), 
                            ifelse(Born == '', '', paste0(Born, ' - ', Died)), ') ', Role),
    Type != '' ~ paste0(Type, ': ', LOCALITY, ', ', Country, ifelse(Note == '', '', paste0('; ', Note))),
    LANGUAGE != '' ~ paste0('After ', LANGUAGE, ifelse(Stem1 == '', '', paste0('; ', Stem1)),
                            ifelse(Stem2 == '', '', paste0(', ', Stem2)),
                            ifelse(Stem3 == '', '', paste0(', ', Stem3)),
                            ifelse(Meaning == '', '', paste0('; Note: ', Meaning))),
    CHEMISTRY != '' ~ paste0(CHEMISTRY, ifelse(`Elements/Ions` == '', '', paste0('. Elements/Ions: ', `Elements/Ions`))),
    `GROUP/INSTITUTION` != '' ~ paste0(`GROUP/INSTITUTION`, ifelse(About == '', '', paste0(' - ', About))),
    other != '' ~ paste0(other, ifelse(note == '', '', paste0(' - ', note)))
  )) %>%
  left_join(ns, by='Mineral_Name') %>%
  mutate(Formula = ifelse(!is.na(Formula), str_replace_all(Formula, '\\_(.*?)\\_',"<sub>\\1</sub>"), NA)) %>%
  mutate(Formula = ifelse(!is.na(Formula), str_replace_all(Formula, '\\^(.*?)\\^',"<sup>\\1</sup>"), NA)) %>%
  select(Mineral_Name, `Approval history`, Groups, Strunz, Formula, `Crystal System`, `Class Name`,
         `H-M symbol`, `Space group`, `Non-standart settings`, Diaphaneity, color, Streak, Luster, Cleavage, Fracture,
         Tenacity, Hardness, `Density measured`, `D(calc,)`, `Habit(main)`, `Named for`, Index_Legend_Label, Index_Legend_Range,
         `Groups Short`, `Type Locality`) %>%
  left_join(mindex, by = c('Mineral_Name' = 'Mineral Name')) %>%
  left_join(e_rocks, by=c('Mineral_Name'='Title')) %>%
  mutate(url_title=tolower(str_replace_all(Mineral_Name, '[()]', ''))) %>%
  mutate(`URL to e-Rocks1` = ifelse(!is.na(Nid),paste0('https://e-rocks.com/node/',Nid), NA)) %>%
  mutate(`URL to e-Rocks` = ifelse(is.na(`URL to e-Rocks`),`URL to e-Rocks1`, `URL to e-Rocks`)) %>%
  mutate(`URL to e-Rocks` = ifelse(Mineral_Name == "O'Danielite",'https://e-rocks.com/minerals/5148/odanielite', `URL to e-Rocks`)) %>%
  select(Mineral_Name, `Approval history`, Groups, Synonyms, Varieties, `Strunz 8th edition`, `Dana 8th edition`, Strunz,
         `Hey's 3rd edition`, Formula, `Crystal System`, `Class Name`,
         `H-M symbol`, `Space group`, `Non-standart settings`, Diaphaneity, color, Streak, Luster, Cleavage, Fracture,
         Tenacity, Hardness, `Density measured`, `D(calc,)`, `Habit(main)`, `Geological occurrence`, `Localities`, `References`,
         `Named for`,`Type Locality`, Index_Legend_Label, Index_Legend_Range, `URL to e-Rocks`, `Context`, `Groups Short`) %>%
  rename(`Strunz 10th edition`=Strunz,`Optical Properties` = Diaphaneity, Colour=color, Lustre=Luster, `Density calculated` = `D(calc,)`,
         Habit = `Habit(main)`, Distribution=Index_Legend_Label, `Distribution Range`=Index_Legend_Range)

mindex_out <- mindex_out %>%
  mutate(Hardness = str_replace_all(Hardness, ',', '.'),
         `Density measured` = str_replace_all(`Density measured`, ',', '.'),
         `Density calculated` = str_replace_all(`Density calculated`, ',', '.'))

mindex_out[mindex_out==""]<-NA
# export CSV --------------------------------------
write.csv(mindex_out, 'Mindex_16112020.csv', na='', quote = F, row.names = F)
write_csv(mindex_out, 'Mindex_24112020.csv', na='', quote_escape = "double")
write_csv(mindex_out, 'Mindex_17022021.csv', na='', quote_escape = "double")


