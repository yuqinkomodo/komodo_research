### LBP Study

library(DBI)
library(odbc)
library(clipr)
library(googlesheets4)
library(tidyverse)
source("~/komodo_research/projects/cost_compare_truven_lbp/helper.r")

# working dir
path <- "~/komodo_research/projects/cost_compare_truven_lbp"
setwd(path)

# create ODBC connection
con_odbc <- DBI::dbConnect(
  odbc::odbc(), "snowflake", 
  role = "ANALYST", 
  warehouse = "LARGE_WH"
)

# google sheet auth
gs4_auth(
  email = gargle::gargle_oauth_email(),
  path = NULL,
  scopes = "https://www.googleapis.com/auth/spreadsheets",
  cache = gargle::gargle_oauth_cache(),
  use_oob = gargle::gargle_oob_default(),
  token = gargle::token_fetch("https://www.googleapis.com/auth/spreadsheets")
)

# specify a googlesheet
ss <- '1vhIyumwQXQnZHn64Rt5X2UOFk6T1U-s-gd_ZyEzXsSQ'

# --- download cohort data --- #
# a patient level file with baseline characteristics
cohort <- 
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "AYWEI", table = "LBP_FIRST_POTENTIAL")
) %>% collect()
colnames(cohort) <- tolower(colnames(cohort))

# cohort funnel summary table in long format
cohort_funnel <- 
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "AYWEI", table = "LBP_COHORT_FUNNEL")
) %>% collect()
colnames(cohort_funnel) <- tolower(colnames(cohort_funnel))

# cohort funnel summary table in long format - by year
cohort_funnel_by_year <- 
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "AYWEI", table = "LBP_COHORT_FUNNEL_BY_YEAR")
) %>% collect()
colnames(cohort_funnel_by_year) <- tolower(colnames(cohort_funnel_by_year))

# a table includes event date for all outcomes for this cohort
cost <-
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "AYWEI", table = "LBP_COST_SUMM")
) %>% collect()
colnames(cost) <- tolower(colnames(cost))

# --- print cohort funnel tables --- #
cohort_funnel_reshape <-
  cohort_funnel %>% 
  arrange(criteria, cohort) %>% 
  pivot_wider(id_cols = "criteria", names_from = c("cohort"), values_from = c("n_bene")) %>%
  mutate(pct_surg = surg/max(surg), pct_non_surg = non_surg/max(non_surg)) %>%
  select(criteria, surg, pct_surg, non_surg, pct_non_surg)
# write_clip() helps copy table into clipboard to paste into googlesheets 
write_clip(cohort_funnel_reshape)

cohort_funnel_by_year_reshape <- 
  cohort_funnel_by_year %>% 
  arrange(criteria, year, cohort) %>% 
  pivot_wider(id_cols = "criteria", names_from = c("year", "cohort"), values_from = c("n_bene"))
write_clip(cohort_funnel_by_year_reshape)

# --- analyze baseline characteristics (table1) --- #
covariates <- get_covariates(cohort)
write_clip(covariates$tbl_bal)
sheet_write(covariates$tbl_bal, ss = ss, sheet = 'covariates')

# Cost
cost_summ <- cost %>%
  mutate(total_cost = mx_cost_pb + mx_cost_fac + rx_cost) %>%
  pivot_longer(cols = c("mx_cost_pb", "mx_cost_fac", "rx_cost", "total_cost"),
               names_to = "category", values_to = "cost") %>%
  group_by(cohort, category) %>%
  summarize(n = n(),
            n_pos = sum(cost > 0), 
            mean = mean(cost), 
            sd = sd(cost),
            min = min(cost),
            p25 = quantile(cost, 0.25),
            median = median(cost),
            p75 = quantile(cost, 0.75),
            max = max(cost))
cost_summ %>% write_clip()

cost_summ_lt_1m <- cost %>%
  mutate(total_cost = mx_cost_pb + mx_cost_fac + rx_cost) %>%
  filter(total_cost <= 5000000) %>%
  pivot_longer(cols = c("mx_cost_pb", "mx_cost_fac", "rx_cost", "total_cost"),
               names_to = "category", values_to = "cost") %>%
  group_by(cohort, category) %>%
  summarize(n = n(),
            n_pos = sum(cost > 0), 
            mean = mean(cost), 
            sd = sd(cost),
            min = min(cost),
            p25 = quantile(cost, 0.25),
            median = median(cost),
            p75 = quantile(cost, 0.75),
            max = max(cost))
cost_summ_lt_1m %>% write_clip()

cost %>% filter(rx_cost > 2000000)