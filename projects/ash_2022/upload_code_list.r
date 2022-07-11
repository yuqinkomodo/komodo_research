# upload covid19 related codes
source("~/komodo_research/library/R/utils.R")
# covid19 diagnosis
# https://docs.google.com/spreadsheets/d/1L3qGc2pqJpFTJbZ4So6uhRTEaPFdsgac7zV4B1K7CTQ/edit#gid=0
# covid19 vaccine
# https://docs.google.com/spreadsheets/d/1HlREpV4D__WFDxfb4qM9jBKfRe9e6anieTuSOC6vx4s/edit#gid=0

con_odbc <- DBI::dbConnect(
  odbc::odbc(), "snowflake",
  role = "ANALYST",
  warehouse = "MEDIUM_WH"
)
ss_diag = '1L3qGc2pqJpFTJbZ4So6uhRTEaPFdsgac7zV4B1K7CTQ'
ss_vac = '1HlREpV4D__WFDxfb4qM9jBKfRe9e6anieTuSOC6vx4s'

bulk_upload_gs(
  conn = con_odbc,
  ss = ss_diag,
  range = 'A:D',
  stage_name = 'stage_kr',
  table_full_name = 'SANDBOX_KOMODO.AYWEI.COVID19_DIAG',
  create_stage = FALSE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'MEDIUM_WH',
  save_sql = TRUE,
  run_sql = TRUE,
  snowsql_profile = 'ywei'
)

# vaccine list
bulk_upload_gs(
  conn = con_odbc,
  ss = ss_vac,
  range = 'A:F',
  stage_name = 'stage_kr',
  table_full_name = 'SANDBOX_KOMODO.AYWEI.COVID19_VAC',
  create_stage = FALSE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'MEDIUM_WH',
  save_sql = TRUE,
  run_sql = TRUE,
  snowsql_profile = 'ywei'
)
