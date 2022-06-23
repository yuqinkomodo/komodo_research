source("~/komodo_research/library/R/utils.R")

con_odbc <- DBI::dbConnect(
  odbc::odbc(), "snowflake",
  role = "ANALYST",
  warehouse = "MEDIUM_WH"
)

bulk_upload(
  conn = con_odbc,
  dir = '~/komodo_research/projects/cost_compare_truven_lbp',
  filename = 'spinal_surgery.csv',
  stage_name = 'stage_kr',
  colClasses = c("character"),
  table_full_name = 'SANDBOX_KOMODO.AYWEI.LBP_DEF_spinal_surgery',
  create_stage = TRUE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'LARGE_WH',
  save_sql = TRUE,
  run_sql = TRUE,
  snowsql_profile = 'ywei'
)
