-- macro get_ce
-- param: sql_ce_mx, sql_ce_rx
with mx as ({sql_ce_mx}), rx as ({sql_ce_rx})
  select mx.upk_key2, 
      greatest(mx.start_date, rx.start_date) as start_date,
      least(mx.end_date, rx.end_date) as end_date
      from mx inner join rx
      on mx.upk_key2 = rx.upk_key2
      where mx.start_date <= rx.end_date and mx.end_date >= rx.start_date