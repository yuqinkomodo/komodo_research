# functions for snowsql
library(glue)

# continuous enrollment
get_ce <- function(bene_input = NULL, mx_version = NULL, grace_period = 45) {
  if (is.null(bene_input) && is.null(mx_version)) stop("Need either bene_input or mx_version!")
  if (is.null(bene_input) && !is.null(mx_version)) {
    bene_input = glue("MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.BENEFICIARY_LS_GA")
  } else if (!is.null(bene_input) && !is.null(mx_version)) {
    bene_input = glue("
        (
            select * from MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.BENEFICIARY_LS_GA
            where upk_key2 in (select distinct upk_key2 from {bene_input})
        )
        ")
  }
  
  sql_ce_mx = get_sql_ce_by_type_kh(bene_input = bene_input, type = "mx", grace_period = grace_period)
  sql_ce_rx = get_sql_ce_by_type_kh(bene_input = bene_input, type = "rx", grace_period = grace_period)
  sql_ce = glue(read_sql("get_ce"))
  return(sql_ce)
}

get_sql_split_fun <- function(grace_period = 45) {
  sql_split = glue(read_sql("split_ranges"))
  sql_split
}

get_sql_ce_by_type_kh <- function(bene_input, type, grace_period = 45){
  if (type == 'mx') {
    cov_ind = 'MEDICAL_COVERAGE_INDICATOR'
  } else if (type == 'rx') {
    cov_ind = 'PHARMACY_COVERAGE_INDICATOR'
  }
  sql_ce = glue(read_sql("get_ce_by_type"))
  sql_ce
}

# query data from mx table
query_event_mx <- function(
  code_list_table,
  patient_table = NULL,
  code_var = "code", type_var = "codetype", event_var = "event",
  mx_version = '20220605', 
  start_date = '2012-01-01', end_date = '2030-12-31',
  keep_vars = NULL
  ) {
  
  keep_vars_str = ""
  if (!is.null(keep_vars)) keep_vars_str = paste(paste0(',', keep_vars), collapse = " ")
  patient_where_str = ""
  if (!is.null(patient_table)) patient_where_str = glue("and upk_key2 in (select upk_key2 from {patient_table})")
  mx_enc_lite = glue("MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.ENCOUNTERSMX_LITE_LS_GA")
  mx_enc = glue("MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.ENCOUNTERSMX_LS_GA")
  mx_line = glue("MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.ENCOUNTERSMX_LS_GA_SERVICE_LINES")
  mx_visit = glue("MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.VISITS_LS_GA")
  sql_query = glue(read_sql("query_event_mx"))
  sql_query
}

# query data from rx table
query_event_rx <- function(
  code_list_table,
  patient_table = NULL,
  code_var = "code", type_var = "codetype", event_var = "event",
  rx_version = '20220613',
  start_date = '2012-01-01', end_date = '2030-12-31',
  keep_vars = NULL
) {
  keep_vars_str = ""
  if (!is.null(keep_vars)) keep_vars_str = paste(paste0(',', keep_vars), collapse = " ")  
  patient_where_str = ""
  if (!is.null(patient_table)) patient_where_str = glue("and upk_key2 in (select upk_key2 from {patient_table})")
  rx_enc = glue("MAP_ENCOUNTERS.RX_ENCOUNTERS_{rx_version}.RX_ENCOUNTER_LS_GA")
  sql_query = glue(read_sql("query_event_rx"))
  sql_query
}

get_adherence <- function(
  rx_table, 
  cohort_table, 
  days_at_risk = 365
) {
  sql_query = glue(read_sql("get_adherence"))
  sql_query
}

get_code <- function(
  input_list,
  method = c('pattern', 'name'),
  type = c('diag', 'proc', 'ndc'), 
  code_version = '20220606'
) {
  method = match.arg(method)
  type = match.arg(type)
  if (method == 'pattern') {
    pattern_str = paste(paste0('\'', input_list, '\''), collapse = ",")
  } else if (method == 'name') {
    pattern_str = paste(paste0('\'%', input_list, '%\''), collapse = ",")
  }
  if (type == 'diag') {
    code_file = glue("MAP_VOCABULARY.RXNORM_{code_version}.V_DIAGNOSIS")
    search_var = ifelse(method == 'pattern', 'code', 'code_description')
  } else if (type == 'proc') {
    code_file = glue("MAP_VOCABULARY.RXNORM_{code_version}.V_PROCEDURE")
    search_var = ifelse(method == 'pattern', 'code', 'code_description')
  } else if (type == 'ndc') {
    code_file = glue("MAP_VOCABULARY.RXNORM_{code_version}.DRUG_MASTER_ACTIVE_AND_HISTORICAL")
    search_var = ifelse(method == 'pattern', 'ndc', 'CUI_L1_NAME')
  }
  sql = glue(read_sql("get_code"))
  sql
}