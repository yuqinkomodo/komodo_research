# metformin vs dpp4 study helper functions
library(tidyverse)
library(table1)
library(cobalt)
library(stringr)
library(survival)
library(ggfortify)
library(survminer)
library(epiR)
library(lubridate)
library(MatchIt)

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
get_covariates <- function(cohort_table) {
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

# summarize study outcomes, mainly incident count/rate but also recurring event counts
# inci_table creates time-to-event variables (outcome, time) indication whether there is an event within study window, and right censor time.
get_outcome <- function(cohort_table, outcome_table, outcome, window, ylim = c(0.99, 1)) {
  # cohort_table <- cohort_post_wo_pdc
  # outcome_table <- outcome_183
  # outcome <- 'hypoglycemia'
  # window <- 183
  # ylim = c(0.99, 1)
  
  recur_table <-
    cohort_table %>%
    left_join(
      outcome_table %>% 
        filter(event == outcome) %>% 
        rename(event_date = claim_date),
      by = 'upk_key2') %>%
    mutate(
      outcome = !is.na(event_date) & (event_date - claim_date) <= window,
      time = as.numeric(pmin(event_date - claim_date, window, na.rm = T))
    ) %>%
    select(upk_key2, claim_date, cohort, outcome, time)
  
  inci_table <-
    cohort_table %>%
    left_join(
      outcome_table %>% 
        filter(event == outcome) %>% 
        rename(event_date = claim_date) %>%
        group_by(upk_key2, event) %>% 
        dplyr::summarize(first_event_date = min(event_date)),
      by = 'upk_key2') %>%
    mutate(
      outcome = !is.na(first_event_date) & (first_event_date - claim_date) <= window,
      time = as.numeric(pmin(first_event_date - claim_date, window, na.rm = T))
    ) 
  #%>%
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

run_matching <- function(seed = 1234, pre_matching_data, pdc_restriction = TRUE, ratio = 1, 
                         path = "~/komodo_research/projects/Metformin_DPP4_V4") {
  set.seed(seed)
  
  if (pdc_restriction) {
    fml <- I(cohort == "dpp4") ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat
    suffix <- "w_pdc"
  } else {
    fml <- I(cohort == "dpp4") ~ age_cat + gender + region + w_ckd + w_obesity + er_cat + cci_cat + pdc
    suffix <- "wo_pdc"
  }
  
  t_start <- proc.time()
  matching_model <- matchit(
    fml,
    data = pre_matching_data,
    ratio = ratio,
    caliper = 0.1
  )
  t_end <- proc.time()
  print(t_end - t_start)
  # saveRDS(matching_model, file.path(path, paste0("matching_", suffix, ".Rds")))
  bal_tbl <- bal.tab(matching_model, addl = ~ year, m.threshold = 0.1, un = T, disp = "means", 
                     s.d.denom = "pooled", binary = "std", continuous = "std")
  bal_plot <- bal.plot(matching_model, "distance", which = 'both')
  love_plot <- love.plot(bal_tbl)
  
  return(list(matching_model = matching_model, bal_nn = bal_tbl$Observations,
              bal_tbl = bal_tbl$Balance, bal_plot = bal_plot, love_plot = love_plot))
}

# run hypothesis testing - try 7 different models... but all expect to return similar results.
# proposed final model is glm using GEE and Cox model, without doubly robust adjustment
run_test <- function(outcome_summary, pdc_restriction = TRUE) {
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
  
  if (pdc_restriction) {
    cov <- c("age_cat", "gender", "region", "w_ckd", "w_obesity", "er_cat", "cci_cat")
  } else {
    cov <- c("age_cat", "gender", "region", "w_ckd", "w_obesity", "er_cat", "cci_cat", "pdc")
  }
  cov_str <- paste(cov, collapse = "+")
  
  # unpaired glm, DR
  m_glm_dr <- glm(as.formula(paste("outcome ~ offset(log(time)) + I(cohort == 'dpp4') + ", cov_str)), family = poisson, data = data)
  summary(m_glm_dr)
  
  # paired glm from gee, DR
  m_gee_dr <- geepack::geeglm(as.formula(paste("outcome ~ offset(log(time)) + I(cohort == 'dpp4') + ", cov_str)), family = poisson, 
                              id = subclass, data = sorted_data, corstr = "exchangeable")
  summary(m_gee_dr)
  
  # paired cox model, DR
  m_cox_dr <- survival::coxph(as.formula(paste("Surv(time, outcome) ~ I(cohort == 'dpp4') + strata(subclass) + ", cov_str)), data = data)
  summary(m_cox_dr)
  
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
               p_val = coef(summary(m_cox))[1, 5]),
    data.frame(method = "glmdr", stat = exp(coef(summary(m_glm_dr))[2, 1]), 
               lb = exp(confint.default(m_glm_dr)[2,1]), ub = exp(confint.default(m_glm_dr)[2,2]),
               p_val = coef(summary(m_glm_dr))[2, 4]),
    data.frame(method = "geedr", stat = exp(coef(summary(m_gee_dr))[2, 1]), 
               lb = exp(confint.default(m_gee_dr)[2,1]), ub = exp(confint.default(m_gee_dr)[2,2]),
               p_val = coef(summary(m_gee_dr))[2, 4]),
    data.frame(method = "coxdr", stat = exp(coef(summary(m_cox_dr))[1, 1]), 
               lb = exp(confint.default(m_cox_dr)[1,1]), ub = exp(confint.default(m_cox_dr)[1,2]),
               p_val = coef(summary(m_cox_dr))[1, 5])
  )
  res
}
