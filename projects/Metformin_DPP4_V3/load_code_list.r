source("~/komodo_research/library/R/utils.R")
con_odbc <- DBI::dbConnect(
  odbc::odbc(), "snowflake",
  role = "ANALYST",
  warehouse = "MEDIUM_WH"
)

bulk_upload(
  conn = con_odbc,
  dir = '~/komodo_research/projects/Metformin_DPP4_V4/code_list',
  filename = 'CLIN-2781_ICD.csv',
  stage_name = 'stage_metdpp4',
  table_full_name = 'SANDBOX_KOMODO.YWEI.METDPP4_DEF_V4',
  create_stage = TRUE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'LARGE_WH',
  save_sql = TRUE,
  run_sql = TRUE,
  snowsql_profile = 'ywei'
)

bulk_upload(
  conn = con_odbc,
  dir = '~/komodo_research/projects/Metformin_DPP4_V4/code_list',
  filename = 'CLIN-2781_ICD.csv',
  stage_name = 'stage_metdpp4',
  table_full_name = 'SANDBOX_KOMODO.YWEI.METDPP4_DEF_NDC_V4',
  create_stage = FALSE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'LARGE_WH',
  save_sql = TRUE,
  run_sql = TRUE,
  snowsql_profile = 'ywei'
)