### utility functions

bulk_upload <- function(
  conn = DBI::ANSI(),
  dir,
  filename, 
  colClasses = NA,
  stage_name,
  table_full_name,
  create_stage = TRUE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'LARGE_WH',
  save_sql = FALSE,
  run_sql = FALSE,
  snowsql_profile = 'ywei'
) {
  
  sql <-
    paste(
      paste("USE", database, ";"),
      paste("USE ROLE", role, ";"),
      paste("USE WAREHOUSE", warehouse, ";"),
    sep = "\n"
  )
  if (create_stage)
    sql <- paste(sql, 
      paste("CREATE OR REPLACE STAGE", stage_name, "FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY=\'\"\' SKIP_HEADER = 1);"), 
      sep = "\n")

  # load file to get colnames and types
  # don't need all rows to guess types
  f <- data.table::fread(file.path(dir, filename), nrows=10000, colClasses = colClasses)
  print(str(f))
  
  # forget there might be a story for integer64, fix later if see anything weird
  # col_classes <- sapply(data.table::fread(file.path(dir, filename), nrows=10000, integer64="character"), class)
  # col_classes[which(col_classes == "integer64")] <- "character"
  # f2 <- data.table::fread(file.path(dir, filename), integer64="character", colClasses = col_classes)
  create_table_str <- paste0(DBI::sqlCreateTable(conn, DBI::SQL(paste0(table_full_name)), f)@.Data, ";")
  
  # remove quotes to create unquote identifier for snowflake
  # may encounter bugs for special chars!
  create_table_str <- stringr::str_remove_all(create_table_str, "\"")
  
  sql <-
    paste(sql,
          paste("PUT", paste0("file://", file.path(dir, filename)), paste0('@', stage_name), 'auto_compress=true overwrite=true;'),
          paste("DROP TABLE IF EXISTS", table_full_name, ";"),
          create_table_str,
          paste("COPY INTO", table_full_name, "FROM", paste0("@", stage_name, "/", filename, ".gz;")),
          sep = "\n"
          )
  
  # print sql commands
  
  cat("\n# bulk upload sql command\n")
  cat(sql)
  # save sql script
  if (save_sql || run_sql) {
    sql_file <- stringr::str_replace(filename, "csv$", "sql")
    conn <- file(file.path(dir, sql_file))
    writeLines(sql, conn)
    close(conn)
  }
  # execute snowsql
  snowsql_cmd <- paste("/Applications/SnowSQL.app/Contents/MacOS/snowsql -c", snowsql_profile)
  snowsql_input <- paste("!source", file.path(dir, sql_file), "\n", "!exit")
  cat("\n# invoke snowsql\n")
  cat(snowsql_cmd)
  cat("\n# snowsql session command\n")
  cat(snowsql_input)
  if (run_sql) {
    system(snowsql_cmd, input = snowsql_input)
  }
  return(invisible(f))
}
# caller
# con_odbc <- DBI::dbConnect(
#   odbc::odbc(), "snowflake", 
#   role = "ANALYST", 
#   warehouse = "MEDIUM_WH"
# )
# 
# bulk_upload(
#   conn = con_odbc,
#   dir = '/Users/yuqin.wei/Projects/Research/DPP4_vs_Metformin',
#   filename = 'diabetes_type1.csv', 
#   stage_name = 'test_stage',
#   table_full_name = 'SANDBOX_KOMODO.YWEI.METDPP4_DEF2',
#   create_stage = TRUE,
#   database = 'SANDBOX_KOMODO',
#   role = 'ANALYST',
#   warehouse = 'LARGE_WH',
#   save_sql = TRUE,
#   run_sql = TRUE,
#   snowsql_profile = 'ywei'
# )


  
