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
        age < 30 ~ '1. 18-29',
        age < 40 ~ '2. 30-39',
        age < 50 ~ '3. 40-49',
        age < 60 ~ '4. 50-59',
        age < 70 ~ '5. 60-69',
        age < 80 ~ '6. 70-79',
        age >= 80 ~ '7. 80+',
        TRUE ~ 'Missing'
      ),
      gender = case_when(
        patient_gender == 'F' ~ 'Female',
        patient_gender == 'M' ~ 'Male',
        TRUE ~ 'Missing'
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
        TRUE ~ 'Missing'
      ),
      year = as.factor(year(index_date))
    ) %>%
    select(upk_key2, cohort, age_cat, gender, region, year)
  tbl_bal <- table1::table1( ~ age_cat + gender + region + year | cohort, data = tbl_clean, extra.col=list(`SMD`=smd))
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