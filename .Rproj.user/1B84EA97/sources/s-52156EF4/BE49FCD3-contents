groups_ions_to_json <- function (input, formula_table, type) {
  
  formula_table <- formula_table %>%
    filter(type == !!type)
  
  input <- input %>%
              left_join(formula_table) %>%
                distinct(Name, .keep_all = TRUE) %>%
                nest(ions=c(A,B,C,D,E,F,X1,X2,X3,Y1,Y2,Y3,V,W,Z)) %>%
                select(Name, Formulae, ions, Note) %>%
                distinct(Name, .keep_all = TRUE) %>%
                mutate(ions = map(ions, function(x) discard(x, .p = is.na))) %>%
                mutate(ions = map(ions, function(x) { 
                  if (length(x) > 0) {
                    positions = names(x)
                    ions_arr = str_split(x, ', ')
                    names(ions_arr) <- positions
                    return(jsonlite::toJSON(ions_arr)) 
                  } else {
                    return(NULL) 
                  }
                })) %>%
                mutate(ions = as.character(ions)) %>%
                na_if(., 'NULL')
  
          return(input)
}

merge_ions = function(input, column_name){
  temp <- input %>%
                select(Name, type, Formulae, Note, !matches(ifelse(str_detect(column_name, 'X1|X2|X3|Y1|Y2|Y3'), column_name, paste0(column_name,'[0-9]'))))
  
  input <- input %>% 
                select(!type) %>%
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
                select(Name, !!column_name) %>%
                left_join(temp, by = 'Name')
  
        rm(temp)
        print(paste0(column_name,' has been processed!'))
        
      return(input)
}


plain_ions = function(input, column_name){
  temp <- input %>%
    select(Id, !matches(ifelse(str_detect(column_name, 'X1|X2|X3|Y1|Y2|Y3'), column_name, paste0(column_name,'[0-9]'))))
  
  input <- input %>% 
    select(Id, matches(ifelse(str_detect(column_name, 'X1|X2|X3|Y1|Y2|Y3'), column_name, paste0(column_name,'[0-9]')))) %>%
    mutate_at(vars(!contains(c('Index', 'Charge', 'Id'))), .funs = function(x){ str_split(x, ', ') }) %>%
    unchop(cols = !contains(c('Index', 'Charge', 'Id'))) %>%
    rowwise() %>%
    mutate_at(vars(!contains(c('Index', 'Charge', 'Id'))), .funs = function(x) {
      colname = quo_name(enquo(x))
      ion = str_extract(colname, '[A-Z][0-9]-?[a-z]?')
      output = list(
        c_across(contains(ion))
      )
      cation = ifelse(is.na(output[[1]][1]), '', output[[1]][1])
      charge = output[[1]][2]
      
      charge = ifelse(str_detect(cation, '\\^') || is.na(charge) || cation == '[box]', '', paste0('^',charge,'^'))
      
      ions_cell = paste0(cation, charge)
      
      return(ifelse(ions_cell == '', NA, ions_cell))
      
    }) %>%
    select(!contains(c('Index', 'Charge'))) %>%
    unite(col = !!column_name, !contains(c('Id')), na.rm = TRUE, sep = ', ') %>%
    group_by(Id) %>%
    summarise(across(!!column_name, function(x) {
      ions = unlist(unique(str_split(x, ', ')))
      return(paste0(ions, collapse = ', '))
    })) %>%
    distinct(Id, .keep_all = TRUE) %>%
    mutate_all(list(~na_if(.,""))) %>%
    ungroup() %>%
    select(Id, !!column_name) %>%
    left_join(temp, by = 'Id')
  
  rm(temp)
  print(paste0(column_name,' has been processed!'))
  
  return(input)
}



