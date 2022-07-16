-- query_event_mx
-- param: keep_vars_str, mx_enc, mx_line, code_list_table, code_var, type_var, patient_where_str
with 
mx_line as (
  select upk_key2, encounter_key, 
    array_agg(procedure) as procedure_array --, ndc_code, revenue_code
  from {mx_line}
  where procedure in (
    select {code_var} from {code_list_table} 
    where upper({type_var}) in ('ICD10PCS', 'ICD9PCS', 'ICD-10-PCS', 'ICD-9-PCS', 'CPT', 'CPT4', 'HCPCS', 'PROCEDURE')
  )
  {patient_where_str}
  group by upk_key2, encounter_key
),
mx_query as (
select upk_key2, claim_date, encounter_key, 
  visit_id, admission_date, discharge_date,
    array_construct_compact(
      d1, d2, d3, d4, d5, d6, d7, d8, d9, d10,
      d11, d12, d13, d14, d15, d16, d17, d18, d19, d20,
      d21, d22, d23, d24, d25, d26
  ) as diagnosis_array, 
   array_construct_compact(
      p1, p2, p3, p4, p5, p6, p7, p8, p9, p10,
      p11, p12, p13, p14, p15, p16, p17, p18, p19, p20,
      p21, p22, p23, p24, p25
  ) as procedure_array
  {keep_vars_str}
from {mx_enc}
where claim_date >= '{start_date}' 
  and claim_date <= '{end_date}' 
  and (
    arrays_overlap(
      diagnosis_array,
      (select array_agg({code_var}) from {code_list_table} where upper({type_var}) in ('ICD10CM', 'ICD9CM', 'ICD-10-CM', 'ICD-9-CM', 'DIAGNOSIS'))
    ) or arrays_overlap(
      procedure_array,
      (select array_agg({code_var}) from {code_list_table} where upper({type_var}) in ('ICD10PCS', 'ICD9PCS', 'ICD-10-PCS', 'ICD-9-PCS', 'CPT', 'CPT4', 'HCPCS', 'PROCEDURE'))
    ) or encounter_key in (select encounter_key from mx_line)
  )
  {patient_where_str}
),
mx_visit_query as (
  select visit_id, visit_start_date, visit_end_date, visit_setting_of_care
  from {mx_visit}
  where visit_id in (select visit_id from mx_query)
),
mx_query_w_visit as (
  select e.upk_key2, e.claim_date, e.encounter_key, e.visit_id, 
    diagnosis_array, 
    array_cat(
      ifnull(e.procedure_array, array_construct()), 
      ifnull(l.procedure_array, array_construct())
      ) as procedure_array,
    admission_date, discharge_date,
    visit_start_date, visit_end_date, visit_setting_of_care
    {keep_vars_str}
  from mx_query e
  left join mx_line l
  on e.encounter_key = l.encounter_key
  left join mx_visit_query v
  on e.visit_id = v.visit_id
)
select upk_key2, claim_date, encounter_key, visit_id, 
  c.{event_var}, c.{code_var}, c.{type_var}, 
  diagnosis_array, procedure_array,
  admission_date, discharge_date,
  visit_start_date, visit_end_date, visit_setting_of_care
  {keep_vars_str}
from mx_query_w_visit q
inner join {code_list_table} c
on (array_contains(c.{code_var}::variant, diagnosis_array) and upper(c.{type_var}) in ('ICD10CM', 'ICD9CM', 'ICD-10-CM', 'ICD-9-CM', 'DIAGNOSIS'))
  or (array_contains(c.{code_var}::variant, procedure_array) and upper(c.{type_var}) in ('ICD10PCS', 'ICD9PCS', 'ICD-10-PCS', 'ICD-9-PCS', 'CPT', 'CPT4', 'HCPCS', 'PROCEDURE'))
