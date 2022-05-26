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
  colClasses = c("character"),
  table_full_name = 'SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF',
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
  filename = 'CLIN-2781_NDC.csv',
  colClasses = c("character","character","logical","character","character","character","logical"),
  stage_name = 'stage_metdpp4',
  table_full_name = 'SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_NDC',
  create_stage = FALSE,
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
  filename = 'CKD_Codes.csv',
  colClasses = c("character","character","character","character","logical"),
  stage_name = 'stage_metdpp4',
  table_full_name = 'SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_CKD',
  create_stage = FALSE,
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
  filename = 'Obesity_Codes.csv',
  colClasses = c("character","character","character","logical"),
  stage_name = 'stage_metdpp4',
  table_full_name = 'SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_OBESITY',
  create_stage = FALSE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'LARGE_WH',
  save_sql = TRUE,
  run_sql = TRUE,
  snowsql_profile = 'ywei'
)