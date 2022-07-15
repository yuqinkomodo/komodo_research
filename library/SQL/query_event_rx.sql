-- query_event_rx
-- param: keep_vars_str, rx_enc, code_list_table, code_var, type_var
with rx_query as (
select upk_key2, claim_date, encounter_key, ndc, days_supply, quantity_dispensed {keep_vars_str}
from {rx_enc}
where claim_date >= '{start_date}' 
    and claim_date <= '{end_date}' 
    and (
      ndc in (select {code_var} from {code_list_table} where upper({type_var}) in ('NDC', 'DRUG'))
    )
    {patient_where_str}
) 
select upk_key2, claim_date, c.{event_var}, c.{code_var}, c.{type_var}, ndc, days_supply, quantity_dispensed {keep_vars_str}
from rx_query q
inner join {code_list_table} c
where q.ndc = c.{code_var} and upper(c.{type_var}) in ('NDC', 'DRUG')
  