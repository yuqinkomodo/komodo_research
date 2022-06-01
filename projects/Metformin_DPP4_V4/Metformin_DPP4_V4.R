### Metformin vs DPP4
# workbook: https://docs.google.com/spreadsheets/d/1347BWfXpZxe0FDfC-JIGxjDfbY-icl51Ve7B_WVxNMk/edit#gid=922542214

# load library
library(DBI)
library(odbc)
library(tidyverse)
library(clipr)
library(table1)
library(cobalt)
library(stringr)
library(survival)
library(ggfortify)
library(survminer)
library(epiR)
library(lubridate)
library(MatchIt)

# create ODBC connection
con_odbc <- DBI::dbConnect(
  odbc::odbc(), "snowflake", 
  role = "ANALYST", 
  warehouse = "LARGE_WH"
)

# download cohort data
cohort_183 <- tbl(con_odbc, 
                Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_FINAL_W_COV_183")
) %>% collect()
colnames(cohort_183) <- tolower(colnames(cohort_183))

cohort_funnel_183 <- tbl(con_odbc, 
                  Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_COHORT_FUNNEL_183")
) %>% collect()
colnames(cohort_funnel_183) <- tolower(colnames(cohort_funnel_183))

cohort_funnel_by_year_183 <- tbl(con_odbc, 
                     Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_COHORT_FUNNEL_183_BY_YEAR")
) %>% collect()
colnames(cohort_funnel_by_year_183) <- tolower(colnames(cohort_funnel_by_year_183))

outcome_183 <- tbl(con_odbc, 
                  Id(database = "SANDBOX_KOMODO", schema = "YWEI", table = "METDPP4_V4_OUTCOME_183")
) %>% collect()
colnames(outcome_183) <- tolower(colnames(outcome_183))

# cohort funnel (by year)
cohort_funnel_reshape <-
  cohort_funnel_183 %>% 
  arrange(criteria, cohort) %>% 
  pivot_wider(id_cols = "criteria", names_from = c("cohort"), values_from = c("n_bene")) %>%
  mutate(pct_dpp4 = dpp4/max(dpp4), pct_metformin = metformin/max(metformin)) %>%
  select(criteria, dpp4, pct_dpp4, metformin, pct_metformin)

write_clip(cohort_funnel_reshape)

cohort_funnel_by_year_reshape <- 
  cohort_funnel_by_year_183 %>% 
  arrange(criteria, year, cohort) %>% 
  pivot_wider(id_cols = "criteria", names_from = c("year", "cohort"), values_from = c("n_bene"))

write_clip(cohort_funnel_by_year_reshape)

# analyze table 1

smd <- function(x, ...) {
  # Construct vectors of data y, and groups (strata) g
  #y <- unlist(x)
  #g <- factor(rep(1:length(x), times=sapply(x, length)))
  print(names(x))
  y0 <- unlist(x[1])
  y1 <- unlist(x[2])
  lvls <- levels(unlist(x))
  if (is.null(lvls)) lvls <- c(TRUE, FALSE)
  print(lvls)
  smd <- sapply(lvls, function(l) {
    p0 <- as.numeric(y0 == l)
    p1 <- as.numeric(y1 == l)
    diff <- mean(p1, na.rm = T) - mean(p0, na.rm = T)
    n0 <- length(y0)
    n1 <- length(y1)
    s <- sqrt(((n0-1)*sd(p0, na.rm = T) + (n1-1)*sd(p1, na.rm = T))/(n0 + n1 - 2))
    smd <- diff/s
  })
  # diff <- mean(y1, na.rm = T) - mean(y0, na.rm = T)
  # n0 <- length(y0)
  # n1 <- length(y1)
  # s <- sqrt(((n0-1)*sd(y0, na.rm = T) + (n1-1)*sd(y1, na.rm = T))/(n0 + n1 - 2))
  # smd <- diff/s  
  c("", formatC(smd, digit = 3, format = "f"))
}
process_cov <- function(cohort_table) {
  tbl_clean <- cohort_table %>%
    mutate(
      age_cat = case_when(
        age < 40 ~ '1. 0-39',
        age < 50 ~ '2. 40-40',
        age < 60 ~ '4. 50-59',
        age < 70 ~ '5. 60-69',
        age < 80 ~ '5. 70-79',
        age >= 80 ~ '6. 80+',
        TRUE ~ '9. Missing'
      ),
      gender = case_when(
        patient_gender == 'F' ~ 'Female',
        patient_gender == 'M' ~ 'Male',
        TRUE ~ 'Missing'
      ),
      cci_cat = case_when(
        charlson_score == 0 ~ '1. 0',
        charlson_score <= 2 ~ '2. 1-2',
        charlson_score <= 4 ~ '3. 3-4',
        charlson_score <= 9 ~ '4. 5-9',
        charlson_score >9 ~ '5. 10+',
        TRUE ~ '0'
      ),
      w_ckd = ckd > 0,
      w_obesity = obesity > 0,
      er_cat = case_when(
        er == 0 ~ '0',
        er == 1 ~ '1',
        er > 1 ~ '2+',
        TRUE ~ '0'
      ),
      region = case_when(
        patient_state %in% c('CT', 'ME', 'MA', 'NH', 'RI', 
                                         'VT', 'NJ', 'NY', 'PA') ~ '1. Northeast',
        patient_state %in% c('IL', 'IN', 'MI', 'OH', 'WI', 
                                         'IA', 'KS', 'MO', 'MN', 'NE', 'ND',
                                         'SD') ~ '2. Midwest',
        patient_state %in% c('DE', 'FL', 'GA', 'MD', 'NC', 
                                         'SC', 'VA', 'DC', 'WV', 'AL',
                                         'KY', 'MS', 'TN', 'AR', 'LA',
                                         'OK', 'TX') ~ '3. South',
        patient_state %in% c('AZ', 'CO', 'ID', 'MT', 'NV', 
                                         'NM', 'UT', 'WY', 'AK', 'CA',
                                         'HI', 'OR', 'WA') ~ '4. West',
        patient_state %in% c('PR', 'VI') ~ '5. US Territories',
        !is.na(patient_state) ~ patient_state,
        TRUE ~ '9. Missing'
      ),
      year = as.factor(year(claim_date)),
      pdc = case_when(
        pdc2 <= 0.4 ~ '0-0.4',
        pdc2 <= 0.8 ~ '0.4-0.8',
        pdc2 > 0.8 ~ '0.8-1',
        TRUE ~ '0-0.4'
      ),
    ) %>%
    select(upk_key2, cohort, age_cat, gender, region, w_ckd, w_obesity, er_cat, cci_cat, year, pdc)
  tbl_bal <- table1::table1( ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat + year  +pdc | cohort, data = tbl_clean, extra.col=list(`SMD`=smd))
  # as.data.frame(tbl_bal)
  #tbl_bal <- bal.tab(cohort ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat, data = tbl_clean,
  #        un = T, disp = "means")
  #tbl_bal
  
  return(list(tbl_clean = tbl_clean, tbl_bal = as.data.frame(tbl_bal)))
}

table_un_wo_pdc <- process_cov(cohort_183)
write_clip(table_un_wo_pdc$tbl_bal)

table_un_w_pdc <- process_cov(cohort_183 %>% filter(pdc2 > 0.8))
write_clip(table_un_w_pdc$tbl_bal)

### patient counts by year-month
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

# outcome analysis
# for simplicity study outcomes within at-risk window first.
get_outcome <- function(cohort_table, outcome_table, outcome, window, ylim = c(0.99, 1)) {
  # cohort_table <- cohort_post
  # outcome_table <- outcome_183
  # outcome <- 'hypoglycemia'
  # window <- 183
  # ylim = c(0.99, 1)
  
  recur_table <-
    cohort_table %>%
    left_join(
      outcome_table %>% filter(event == outcome) %>% rename(event_date = claim_date)
      , by = 'upk_key2') %>%
    mutate(
      outcome = !is.na(event_date) & (event_date - claim_date) <= window,
      time = as.numeric(pmin(event_date - claim_date, window, na.rm = T))
    ) %>%
    select(upk_key2, claim_date, cohort, outcome, time)
  
  inci_table <-
    cohort_table %>%
    left_join(
      outcome_table %>% filter(event == outcome) %>% rename(event_date = claim_date) %>%
        group_by(upk_key2, event) %>% summarize(first_event_date = min(event_date))
      , by = 'upk_key2') %>%
    mutate(
      outcome = !is.na(first_event_date) & (first_event_date - claim_date) <= window,
      time = as.numeric(pmin(first_event_date - claim_date, window, na.rm = T))
    ) #%>%
    #select(upk_key2, claim_date, cohort, outcome, time)

  summ_table <- inci_table %>%
    group_by(cohort) %>%
    summarize(n_patient = n(), total_time = sum(time)/365, n_incidence = sum(outcome)) %>%
    left_join(
      recur_table %>%
        group_by(cohort) %>%
        summarize(n_recurring = sum(outcome))
    ) %>%
    mutate(
      incidence_rate = n_incidence/as.numeric(total_time)*1000,
      prevalence_proportion = n_incidence/n_patient,
      recurring_event_rate = n_recurring/as.numeric(total_time)*1000
    )
  
  fit <- survfit(Surv(time, outcome) ~ cohort, data = inci_table)
  plot <- 
    ggsurvplot(fit, pval = FALSE, conf.int = TRUE, ggtheme = theme_minimal(),
               title = paste0("Kaplan-Miere Estimator ","(At-risk window:", window, " Outcome:", outcome, ")"),
               xlim = c(0, window), ylim = ylim)
  return(list(inci_table = inci_table, summ_table = summ_table, fit = fit, plot = plot))
}

outcome_summ_pre_hypo <- get_outcome(cohort_183, outcome_183, "hypoglycemia", 183, ylim = c(0.99, 1))
outcome_summ_pre_hypo$summ_table %>% write_clip()
outcome_summ_pre_hypo$plot

outcome_summ_pre_er <- get_outcome(cohort_183, outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_pre_er$summ_table %>% write_clip()
outcome_summ_pre_er$plot

outcome_summ_pre_pdc_hypo <- get_outcome(cohort_183 %>% filter(pdc2 > 0.8), outcome_183, "hypoglycemia", 183, ylim = c(0.99, 1))
outcome_summ_pre_pdc_hypo$summ_table %>% write_clip()
outcome_summ_pre_pdc_hypo$plot

outcome_summ_pre_pdc_er <- get_outcome(cohort_183 %>% filter(pdc2 > 0.8), outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_pre_pdc_er$summ_table %>% write_clip()
outcome_summ_pre_pdc_er$plot

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

# propensity score matching
pre_matching_wo_pdc <- table_un_wo_pdc$tbl_clean %>% 
  filter(age_cat != '9. Missing', gender != 'Missing', !region %in% c('5. US Territories', '9. Missing'))
pre_matching_w_pdc <- pre_matching_wo_pdc %>%
  filter(pdc == '0.8-1')

set.seed(5678)
system.time(matching_model_w_pdc <- matchit(
  I(cohort == "dpp4") ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat,
  data = pre_matching_w_pdc,
  ratio = 1,
  caliper = 0.1
))
saveRDS(matching_model_w_pdc, "/Users/yuqin.wei/komodo_research/projects/Metformin_DPP4_V4/matching_w_pdc.Rds")
bal_tbl_w_pdc <- bal.tab(matching_model_w_pdc, addl = ~ year, m.threshold = 0.1, un = T, disp = "means")
bal_plot_w_pdc <- bal.plot(matching_model_w_pdc, "distance", which = 'both')
love_plot_w_pdc <- love.plot(bal_tbl_w_pdc)

bal_tbl_w_pdc$Observations %>% write_clip()
bal_tbl_w_pdc$Balance %>% write_clip()
bal_plot_w_pdc
love_plot_w_pdc

set.seed(1234)
system.time(matching_model_wo_pdc <- matchit(
  I(cohort == "dpp4") ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat + pdc,
  data = pre_matching_wo_pdc,
  ratio = 1,
  caliper = 0.1
))
saveRDS(matching_model_wo_pdc, "/Users/yuqin.wei/komodo_research/projects/Metformin_DPP4_V4/matching_wo_pdc.Rds")
bal_tbl_wo_pdc <- bal.tab(matching_model_wo_pdc, addl = ~ year, m.threshold = 0.1, un = T, disp = "means")
bal_plot_wo_pdc <- bal.plot(matching_model_wo_pdc, "distance", which = 'both')
love_plot_wo_pdc <- love.plot(bal_tbl_wo_pdc)

bal_tbl_wo_pdc$Observations %>% write_clip()
bal_tbl_wo_pdc$Balance %>% write_clip()
bal_plot_wo_pdc
love_plot_wo_pdc

# try a 1:2 matching

set.seed(5678)
system.time(matching_model_w_pdc_2 <- matchit(
  I(cohort == "dpp4") ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat,
  data = pre_matching_w_pdc,
  ratio = 2,
  caliper = 0.1
))
saveRDS(matching_model_w_pdc_2, "/Users/yuqin.wei/komodo_research/projects/Metformin_DPP4_V4/matching_w_pdc_2.Rds")
bal_tbl_w_pdc_2 <- bal.tab(matching_model_w_pdc_2, addl = ~ year, m.threshold = 0.1, un = T, disp = "means")
bal_plot_w_pdc_2 <- bal.plot(matching_model_w_pdc_2, "distance", which = 'both')
love_plot_w_pdc_2 <- love.plot(bal_tbl_w_pdc_2)

bal_tbl_w_pdc_2$Observations %>% write_clip()
bal_tbl_w_pdc_2$Balance %>% write_clip()
bal_plot_w_pdc_2
love_plot_w_pdc_2

set.seed(1234)
system.time(matching_model_wo_pdc_2 <- matchit(
  I(cohort == "dpp4") ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat + pdc,
  data = pre_matching_wo_pdc,
  ratio = 2,
  caliper = 0.1
))
saveRDS(matching_model_wo_pdc_2, "/Users/yuqin.wei/komodo_research/projects/Metformin_DPP4_V4/matching_wo_pdc_2.Rds")
bal_tbl_wo_pdc_2 <- bal.tab(matching_model_wo_pdc_2, addl = ~ year, m.threshold = 0.1, un = T, disp = "means")
bal_plot_wo_pdc_2 <- bal.plot(matching_model_wo_pdc_2, "distance", which = 'both')
love_plot_wo_pdc_2 <- love.plot(bal_tbl_wo_pdc_2)

bal_tbl_wo_pdc_2$Observations %>% write_clip()
bal_tbl_wo_pdc_2$Balance %>% write_clip()
bal_plot_wo_pdc_2
love_plot_wo_pdc_2

# outcome summary after matching
cohort_post_w_pdc <- match.data(matching_model_w_pdc) %>%
  left_join(cohort_183 %>% select(upk_key2, claim_date))
cohort_post_wo_pdc <- match.data(matching_model_wo_pdc) %>%
  left_join(cohort_183 %>% select(upk_key2, claim_date))

outcome_summ_post_hypo <- get_outcome(cohort_post_wo_pdc, outcome_183, "hypoglycemia", 183, ylim = c(0.98, 1))
outcome_summ_post_hypo$summ_table %>% write_clip()
outcome_summ_post_hypo$plot

outcome_summ_post_er <- get_outcome(cohort_post_wo_pdc, outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_post_er$summ_table %>% write_clip()
outcome_summ_post_er$plot

outcome_summ_post_pdc_hypo <- get_outcome(cohort_post_w_pdc, outcome_183, "hypoglycemia", 183, ylim = c(0.99, 1))
outcome_summ_post_pdc_hypo$summ_table %>% write_clip()
outcome_summ_post_pdc_hypo$plot

outcome_summ_post_pdc_er <- get_outcome(cohort_post_w_pdc, outcome_183, "er", 183, ylim = c(0.85, 1))
outcome_summ_post_pdc_er$summ_table %>% write_clip()
outcome_summ_post_pdc_er$plot

# Hypothesis testing
run_test <- function(outcome_summary) {
  # outcome_summary <- outcome_summ_post_hypo
  
  data <- outcome_summary$inci_table
  x <- outcome_summary$summ_table$n_incidence
  t <- outcome_summary$summ_table$total_time
  
  # unpaired rate ratio test
  m_rr <- fmsb::rateratio(x[1], x[2], t[1], t[2])
  
  # unpaired glm
  m_glm <- glm(outcome ~ offset(log(time)) + I(cohort == "dpp4"), family = poisson, data = data)
  summary(m_glm)
  
  # paired glm from gee
  sorted_data <- data %>% arrange(subclass, cohort)
  sorted_data$subclass <- as.numeric(sorted_data$subclass)
  m_gee <- geepack::geeglm(outcome ~ offset(log(time)) + I(cohort == "dpp4"), family = poisson, 
                           id = subclass, data = sorted_data, corstr = "exchangeable")
  summary(m_gee)
  
  # paired cox model
  m_cox <- survival::coxph(Surv(time, outcome) ~ I(cohort == "dpp4") + strata(subclass), data = data)
  summary(m_cox)
  
  res <- rbind(
    data.frame(method = "irr", stat = m_rr$estimate, lb = m_rr$conf.int[1], ub = m_rr$conf.int[2], p_val = m_rr$p.value),
    data.frame(method = "glm", stat = exp(coef(summary(m_glm))[2, 1]), 
               lb = exp(confint.default(m_glm)[2,1]), ub = exp(confint.default(m_glm)[2,2]),
               p_val = coef(summary(m_glm))[2, 4]),
    data.frame(method = "gee", stat = exp(coef(summary(m_gee))[2, 1]), 
               lb = exp(confint.default(m_gee)[2,1]), ub = exp(confint.default(m_gee)[2,2]),
               p_val = coef(summary(m_gee))[2, 4]),
    data.frame(method = "cox", stat = exp(coef(summary(m_cox))[1, 1]), 
               lb = exp(confint.default(m_cox)[1,1]), ub = exp(confint.default(m_cox)[1,2]),
               p_val = coef(summary(m_cox))[1, 5])
  )
  res
}
test_wo_odc_hypo <- run_test(outcome_summ_post_hypo)
test_wo_odc_er <- run_test(outcome_summ_post_er)
test_w_odc_hypo <- run_test(outcome_summ_post_pdc_hypo)
test_w_odc_er <- run_test(outcome_summ_post_pdc_er)

test_wo_odc_hypo %>% write_clip()
test_wo_odc_er %>% write_clip()
test_w_odc_hypo %>% write_clip()
test_w_odc_er %>% write_clip()
