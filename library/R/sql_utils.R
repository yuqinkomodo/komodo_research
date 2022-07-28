# utility functions calling snowflake query
read_query <- function(sql, conn = con_odbc) {
  df <- DBI::dbGetQuery(conn, sql)
  colnames(df) <- tolower(colnames(df))
  df
}

read_table <- function(..., conn = con_odbc) {
  # tyipical input of ... expects:
  # database, schema, table
  tbl(conn, DBI::Id(...))
}

execute_sql <- function(sql, conn = con_odbc, print = FALSE) {
  if (print) cat(sql)
  DBI::dbExecute(conn, sql)
  return(invisible(NULL))
}

# EY: original script code doesn't work if this path changes
read_sql <- function(file, path = "~/komodo/code/komodo_research/library/SQL") {
  if (!stringr::str_ends(file, fixed(".sql", TRUE))) file <- paste0(file, ".sql")
  readr::read_file(file.path(path, file))
}
