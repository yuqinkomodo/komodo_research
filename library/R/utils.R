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

# ss = '1PnYunOisk7FDR6v0o4asAFBD6vbi3ChtTliVl-sYmU8'

bulk_upload_gs <- function(
  conn = DBI::ANSI(),
  url = NULL,
  ss = NULL,
  sheet = NULL,
  range = NULL,
  table_full_name,
  stage_name,
  create_stage = FALSE,
  database = 'SANDBOX_KOMODO',
  role = 'ANALYST',
  warehouse = 'LARGE_WH',
  save_sql = FALSE,
  run_sql = FALSE,
  snowsql_profile = 'ywei'
) {
  require(googlesheets4)
  require(dplyr)
  require(stringr)
  
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
  
  if (is.null(url) && is.null(ss)) stop("Needs either url or ss to continue.")
  if (is.null(ss)) ss <- as_sheets_id(url)
  
  # need to call this with token parameter
  gs4_auth(
    email = gargle::gargle_oauth_email(),
    path = NULL,
    scopes = "https://www.googleapis.com/auth/spreadsheets",
    cache = gargle::gargle_oauth_cache(),
    use_oob = gargle::gargle_oob_default(),
    token = gargle::token_fetch("https://www.googleapis.com/auth/spreadsheets")
  )
  # f <- range_read(ss, sheet = sheet, range = range)
  f <- range_speedread(ss, sheet = sheet, range = range, guess_max = 10000)
  
  # clean up the sheet, remove space, remove columns start with ..., truncate string > 255
  f <- f %>% select(names(f)[!str_detect(names(f), "^[\\.]{3}")])
  names(f) <- str_remove(names(f), " ")
  for (col in names(f)) {
    if (class(f[[col]]) == "character") {
      f[[col]] <- str_trunc(f[[col]], 255)
    }
  }
  
  # save the file in tempdir()
  readr::write_csv(f, file = file.path(tempdir(), paste0(table_full_name, ".csv")))
  
  create_table_str <- paste0(DBI::sqlCreateTable(conn, DBI::SQL(paste0(table_full_name)), f)@.Data, ";")
  # remove quotes to create unquote identifier for snowflake
  # may encounter bugs for special chars!
  create_table_str <- stringr::str_remove_all(create_table_str, "\"")
  
  dir <- tempdir()
  filename <- paste0(table_full_name, ".csv")
  
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





# helper function pass to table1::table1()
# input: x - a list of covariates for each cohort.
smd <- function(x, ...) {
  # print(names(x))
  # grab vectors of covariate values for cohort 0 and 1
  y0 <- unlist(x[1])
  y1 <- unlist(x[2])
  # for categorical variable, get all levels
  lvls <- levels(unlist(x))
  
  if (is.null(lvls)) lvls <- c(TRUE, FALSE)
  # print(lvls)
  smd <- sapply(lvls, function(l) {
    x0 <- as.numeric(y0 == l)
    x1 <- as.numeric(y1 == l)
    v0 <- var(x0, na.rm = T)
    v1 <- var(x1, na.rm = T)
    n0 <- length(x0)
    n1 <- length(x1)
    
    diff <- mean(v1, na.rm = T) - mean(v0, na.rm = T)
    s <- sqrt(((n0-1)*v0 + (n1-1)*v1)/(n0 + n1 - 2))
    smd <- diff/s
    # cat(diff, v0, v1, n0, n1, s, smd, "\n")
    smd
  })
  c("", formatC(smd, digit = 3, format = "f"))
}

# clean and format covariates
get_covariates <- function(cohort_table, covariates) {
  tbl_clean <- cohort_table %>%
    select(upk_key2, index_date, cohort, all_of(covariates))
  formula <- as.formula(glue("~ {paste(covariates, collapse = '+')}|cohort"))
  tbl_bal <- table1::table1(formula, data = tbl_clean, extra.col=list(`SMD`=smd))
  return(list(tbl_clean = tbl_clean, tbl_bal = as.data.frame(tbl_bal)))
}

# run matching model and model summary

run_matching <- function(
  seed = 1234, pre_matching_data, ratio = 1, caliper = 0.2, 
  matching_vars, exact_matching_vars, summary_vars = NULL) {
  require(cobalt)
  require(MatchIt)
  set.seed(seed)
  fml_matching <- as.formula(glue("cohort ~ {paste(matching_vars, collapse = '+')}"))
  fml_exact <- NULL
  if (!is.null(exact_matching_vars)) fml_exact <- as.formula(glue(" ~ {paste(exact_matching_vars, collapse = '+')}"))
  fml_summary <- NULL
  if (!is.null(summary_vars)) fml_summary <- as.formula(glue(" ~ {paste(summary_vars, collapse = '+')}"))
  t_start <- proc.time()
  matching_model <- matchit(
    fml_matching,
    data = pre_matching_data,
    ratio = ratio,
    caliper = caliper,
    exact = fml_exact
  )
  t_end <- proc.time()
  print(t_end - t_start)
  bal_tbl <- bal.tab(matching_model, addl = fml_summary, m.threshold = 0.1, un = T, disp = "means", 
                     s.d.denom = "pooled", binary = "std", continuous = "std")
  bal_plot <- bal.plot(matching_model, "distance", which = 'both')
  love_plot <- love.plot(bal_tbl)
  
  return(list(matching_model = matching_model, bal_nn = bal_tbl$Observations,
              bal_tbl = bal_tbl$Balance, bal_plot = bal_plot, love_plot = love_plot))
}
