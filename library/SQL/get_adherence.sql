-- get_adherence
-- param rx_table, cohort_table, days_at_risk
with 
input_rx as (
  select upk_key2, cohort, claim_date, days_supply
  from {rx_table}
)
, distinct_claims as (
    select distinct c.upk_key2, c.cohort, c.index_date,
        r.claim_date as fill_start_date,
        case
            when r.days_supply <= 3 then 30
            when r.days_supply >= 360 then 30
            else r.days_supply
        end as rx_days_supply_clean
    from {cohort_table} c
    left join input_rx r
    on c.upk_key2 = r.upk_key2 and c.cohort = r.cohort 
        and r.claim_date between c.index_date and c.index_date + {days_at_risk}
)
-- Patient aggregations
, patient_aggs as (
select *
   , min(fill_start_date) over (partition by upk_key2) as first_period_date
   , dateadd('day', rx_days_supply_clean, fill_start_date) as fill_end_date
   , fill_end_date - fill_start_date as revised_days_supply
   , max(fill_end_date) over (partition by upk_key2) as last_period_date
   , count(*) over (partition by upk_key2) as n_claims
  from distinct_claims
)
-- For each patient, calculate non-overlapping time frames between fill_start_date and fill_end_date
, pdc_dates as (
    select s1.upk_key2,
           s1.cohort,
           s1.first_period_date,
           s1.fill_start_date,
           MIN(t1.fill_end_date) as min_fill_end_date,
           datediff('day', s1.fill_start_date, least(min_fill_end_date, s1.first_period_date + {days_at_risk} - 1)) as covered_days
    from patient_aggs s1
    inner join patient_aggs t1 on s1.fill_start_date <= t1.fill_end_date
                                  and s1.upk_key2 = t1.upk_key2
    and not exists (select * from patient_aggs t2
                   where t1.fill_end_date >= t2.fill_start_date and t1.fill_end_date < t2.fill_end_date
                   and t1.upk_key2 = t2.upk_key2
                   )
    where not exists (select * from patient_aggs s2
                     where s1.fill_start_date > s2.fill_start_date and s1.fill_start_date <= s2.fill_end_date
                     and s1.upk_key2 = s2.upk_key2
                     )
    group by 1,2,3,4
    order by s1.fill_start_date)
--Calculate numerator for PDC
, pdc_results as (
  select upk_key2,
    sum(covered_days) as days_in_period_covered
  from pdc_dates
  group by 1
)
select distinct dc.upk_key2, dc.cohort,
    index_date,
    n_claims,
    first_period_date,
    last_period_date,
    datediff('day', first_period_date, last_period_date) as days_in_period,
    {days_at_risk} as days_at_risk,
    days_in_period_covered,
    sum(revised_days_supply) over (partition by dc.upk_key2) as sum_days_supply,
    round(sum_days_supply/days_in_period,2) as mpr,
    round(days_in_period_covered/days_in_period,2) as pdc,
    round(least(days_in_period_covered, {days_at_risk})/days_at_risk,2) as pdc_at_risk,
    iff(mpr > 0.8, TRUE, FALSE) as mpr_compliant,
    iff(pdc > 0.8, TRUE, FALSE) as pdc_compliant,
    iff(pdc_at_risk > 0.8, TRUE, FALSE) as pdc_at_risk_compliant
from patient_aggs dc
left join pdc_results pdc on pdc.upk_key2 = dc.upk_key2