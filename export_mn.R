# Masterlist2 ------------------------------------------------------------------------------


# read Status data
initial <- googlesheets4::read_sheet(
  ss='1QA-Y229WNurpJA7KYmpiU2jn-_YJmrUfLq_lv0VFpXg',
  sheet = 'Status data',
  col_names = TRUE,
  na = ""
)


mn_minerals <- initial %>%
  select(Mineral_Name) %>%
  distinct(Mineral_Name)

mn_statuses <- initial %>%
  select(Mineral_Name, all_indexes) %>%
  separate_rows(all_indexes, sep = '; ')

mn_relations <- initial %>%
  select(Mineral_Name, `A synonym of`, `A variety of`, `A polytype of`,
         `A mixture of`) %>%
  separate_rows(`A synonym of`, sep = ', |; ') %>%
  separate_rows(`A variety of`, sep = ', |; ') %>%
  separate_rows(`A polytype of`, sep = ', |; ') %>%
  separate_rows(`A mixture of`, sep = ', |; ') %>%
  filter(!is.na(`A synonym of`) | !is.na(`A variety of`) | !is.na(`A polytype of`) | !is.na(`A mixture of`))


minerals <- initial %>%
  filter(!is.na(Minerals_Names)) %>%
  select(Minerals_Names, Formulae) %>%
  distinct(Minerals_Names, .keep_all = TRUE)



#MINERALS
write_csv(mn_statuses, path = 'mn_statuses.csv', na='')
write_csv(mn_minerals, path = 'mn_minerals.csv', na='')
write_csv(mn_relations, path = 'mn_relations.csv', na='')