### Metformin vs DPP4 V4 
# workbook: https://docs.google.com/spreadsheets/d/1347BWfXpZxe0FDfC-JIGxjDfbY-icl51Ve7B_WVxNMk/edit#gid=922542214

library(DBI)
library(odbc)
library(clipr)
library(tidyverse)
source("~/komodo_research/projects/Metformin_DPP4_V4/helper.r")

# working dir
path <- "~/komodo_research/projects/Metformin_DPP4_V4"
setwd(path)

# create ODBC connection
con_odbc <- DBI::dbConnect(
  odbc::odbc(), "snowflake", 
  role = "ANALYST", 
  warehouse = "LARGE_WH"
)

# --- download cohort data --- #
# a patient level file with baseline characteristics
cohort_183 <- 
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_FINAL_W_COV_183")
) %>% collect()
colnames(cohort_183) <- tolower(colnames(cohort_183))

# cohort funnel summary table in long format
cohort_funnel_183 <- 
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_COHORT_FUNNEL_183")
) %>% collect()
colnames(cohort_funnel_183) <- tolower(colnames(cohort_funnel_183))

# cohort funnel summary table in long format - by year
cohort_funnel_by_year_183 <- 
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_COHORT_FUNNEL_183_BY_YEAR")
) %>% collect()
colnames(cohort_funnel_by_year_183) <- tolower(colnames(cohort_funnel_by_year_183))

# a table includes event date for all outcomes for this cohort
outcome_183 <- 
  tbl(con_odbc, Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_OUTCOME_183")
) %>% collect()
colnames(outcome_183) <- tolower(colnames(outcome_183))

# --- print cohort funnel tables --- #
cohort_funnel_reshape <-
  cohort_funnel_183 %>% 
  arrange(criteria, cohort) %>% 
  pivot_wider(id_cols = "criteria", names_from = c("cohort"), values_from = c("n_bene")) %>%
  mutate(pct_dpp4 = dpp4/max(dpp4), pct_metformin = metformin/max(metformin)) %>%
  select(criteria, dpp4, pct_dpp4, metformin, pct_metformin)
# write_clip() helps copy table into clipboard to paste into googlesheets 
write_clip(cohort_funnel_reshape)

cohort_funnel_by_year_reshape <- 
  cohort_funnel_by_year_183 %>% 
  arrange(criteria, year, cohort) %>% 
  pivot_wider(id_cols = "criteria", names_from = c("year", "cohort"), values_from = c("n_bene"))
write_clip(cohort_funnel_by_year_reshape)

# --- analyze baseline characteristics (table1) --- #
table_pre_wo_pdc <- get_covariates(cohort_183)
write_clip(table_pre_wo_pdc$tbl_bal)

table_pre_w_pdc <- get_covariates(cohort_183 %>% filter(pdc2 > 0.8))
write_clip(table_pre_w_pdc$tbl_bal)

# --- get patient counts by year-month --- #
cohort_by_time_wo_pdc <-   
  cohort_183 %>% 
  mutate(year = year(claim_date), month = month(claim_date)) %>%
  group_by(year, month, cohort) %>%
  summarize(n_patient = n()) %>%
  pivot_wider(id_cols = c("year", "month"), names_from = "cohort", values_from = "n_patient")

cohort_by_time_w_pdc <-   
  cohort_183 %>% 
  filter(pdc2 > 0.8) %>%
  mutate(year = year(claim_date), month = month(claim_date)) %>%
  group_by(year, month, cohort) %>%
  summarize(n_patient = n()) %>%
  pivot_wider(id_cols = c("year", "month"), names_from = "cohort", values_from = "n_patient")

write_clip(cohort_by_time_w_pdc)
write_clip(cohort_by_time_wo_pdc)

# --- summarize study outcomes (pre-matching) --- #
outcome_summ_pre_wo_pdc_hypo <- get_outcome(cohort_183, outcome_183, "hypoglycemia", 183, ylim = c(0.99, 1))
outcome_summ_pre_wo_pdc_hypo$summ_table %>% write_clip()
outcome_summ_pre_wo_pdc_hypo$plot

outcome_summ_pre_wo_pdc_er <- get_outcome(cohort_183, outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_pre_wo_pdc_er$summ_table %>% write_clip()
outcome_summ_pre_wo_pdc_er$plot

outcome_summ_pre_w_pdc_hypo <- get_outcome(cohort_183 %>% filter(pdc2 > 0.8), outcome_183, "hypoglycemia", 183, ylim = c(0.99, 1))
outcome_summ_pre_w_pdc_hypo$summ_table %>% write_clip()
outcome_summ_pre_w_pdc_hypo$plot

outcome_summ_pre_w_pdc_er <- get_outcome(cohort_183 %>% filter(pdc2 > 0.8), outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_pre_w_pdc_er$summ_table %>% write_clip()
outcome_summ_pre_w_pdc_er$plot

# --- propensity score matching --- #
# get rid of small categories
pre_matching_wo_pdc <- table_pre_wo_pdc$tbl_clean %>% 
  filter(age_cat != '9. Missing', gender != 'Missing', !region %in% c('5. US Territories', '9. Missing'))
pre_matching_w_pdc <- table_pre_w_pdc$tbl_clean %>% 
  filter(age_cat != '9. Missing', gender != 'Missing', !region %in% c('5. US Territories', '9. Missing'))

# matching model for the cohort w/ pdc restriction takes 15 minutes.
# matching model for the cohort w/o pdc takes 2h +. Objects are saved locally.
matching_model_w_pdc_1 <- run_matching(seed = 5678, pre_matching_w_pdc, pdc_restriction = TRUE, ratio = 1)
saveRDS(matching_model_w_pdc_1, file.path(path, paste0("matching_w_pdc_1.Rds")))
matching_model_wo_pdc_1 <- run_matching(seed = 1234, pre_matching_wo_pdc, pdc_restriction = FALSE, ratio = 1)
saveRDS(matching_model_wo_pdc_1, file.path(path, paste0("matching_wo_pdc_1.Rds")))
matching_model_w_pdc_2 <- run_matching(seed = 5678, pre_matching_w_pdc, pdc_restriction = TRUE, ratio = 2)
saveRDS(matching_model_w_pdc_2, file.path(path, paste0("matching_w_pdc_2.Rds")))
matching_model_wo_pdc_2 <- run_matching(seed = 1234, pre_matching_wo_pdc, pdc_restriction = FALSE, ratio = 2)
saveRDS(matching_model_wo_pdc_2, file.path(path, paste0("matching_wo_pdc_2.Rds")))

# --- summarize study outcomes (post-matching) --- #
if (!exists("matching_model_w_pdc_1"))
  matching_model_w_pdc_1 <- readRDS(file.path(path, paste0("matching_w_pdc_1.Rds")))
if (!exists("matching_model_wo_pdc_1"))
  matching_model_wo_pdc_1 <- readRDS(file.path(path, paste0("matching_wo_pdc_1.Rds")))
if (!exists("matching_model_w_pdc_2"))
  matching_model_w_pdc_2 <- readRDS(file.path(path, paste0("matching_w_pdc_2.Rds")))
if (!exists("matching_model_wo_pdc_2"))
  matching_model_wo_pdc_2 <- readRDS(file.path(path, paste0("matching_wo_pdc_2.Rds")))

cohort_post_w_pdc_1 <- match.data(matching_model_w_pdc_1$matching_model) %>%
  left_join(cohort_183 %>% select(upk_key2, claim_date))
cohort_post_wo_pdc_1 <- match.data(matching_model_wo_pdc_1$matching_model) %>%
  left_join(cohort_183 %>% select(upk_key2, claim_date))

outcome_summ_post_wo_pdc_hypo_1 <- get_outcome(cohort_post_wo_pdc_1, outcome_183, "hypoglycemia", 183, ylim = c(0.98, 1))
outcome_summ_post_wo_pdc_hypo_1$summ_table %>% write_clip()
outcome_summ_post_wo_pdc_hypo_1$plot

outcome_summ_post_wo_pdc_er_1 <- get_outcome(cohort_post_wo_pdc_1, outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_post_wo_pdc_er_1$summ_table %>% write_clip()
outcome_summ_post_wo_pdc_er_1$plot

outcome_summ_post_w_pdc_hypo_1 <- get_outcome(cohort_post_w_pdc_1, outcome_183, "hypoglycemia", 183, ylim = c(0.99, 1))
outcome_summ_post_w_pdc_hypo_1$summ_table %>% write_clip()
outcome_summ_post_w_pdc_hypo_1$plot

outcome_summ_post_w_pdc_er_1 <- get_outcome(cohort_post_w_pdc_1, outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_post_w_pdc_er_1$summ_table %>% write_clip()
outcome_summ_post_w_pdc_er_1$plot

# --- Hypothesis testing --- #
test_wo_odc_hypo_1 <- run_test(outcome_summ_post_wo_pdc_hypo_1, pdc_restriction = FALSE)
test_wo_odc_er_1 <- run_test(outcome_summ_post_wo_pdc_er_1, pdc_restriction = FALSE)
test_w_odc_hypo_1 <- run_test(outcome_summ_post_w_pdc_hypo_1, pdc_restriction = TRUE)
test_w_odc_er_1 <- run_test(outcome_summ_post_w_pdc_er_1, pdc_restriction = TRUE)

test_wo_odc_hypo %>% write_clip()
test_wo_odc_er %>% write_clip()
test_w_odc_hypo %>% write_clip()
test_w_odc_er %>% write_clip()

### power analysis - experimental
# epi.sscohortt(irexp1 = outcome_summ_183_er$summ_table$ir[2]/1000, 
#               FT = NA, power = 0.80, 
#               r = 1, design = 1, sided.test = 2, 
#               nfractional = FALSE, conf.level = 0.95)
# 
# epi.sscohortt(irexp0 = outcome_summ_183_er$summ_table$ir[2]/1000, 
#               FT = 0.5, power = 0.80, 
#               r = 1, design = 1, sided.test = 2, 
#               nfractional = FALSE, conf.level = 0.95)
