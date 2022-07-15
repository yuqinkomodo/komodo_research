-- macro: get_ce_by_type
-- param: cov_ind
with b as (
    SELECT distinct upk_key2, 
    closed_start_date AS start_date_date, 
    date_part('EPOCH_SECOND', to_timestamp(start_date_date)) as start_date, 
    least(closed_end_date, current_date) as end_date_date,
    date_part('EPOCH_SECOND', to_timestamp(end_date_date)) as end_date, 
    max(end_date) over (partition by upk_key2, start_date) as max_end_date, 
    min(start_date) over (partition by upk_key2, end_date) as min_start_date
FROM {bene_input}
WHERE {cov_ind} 
    and closed_start_date IS NOT NULL 
    AND closed_end_date IS NOT NULL 
    AND start_date <= end_date 
    AND start_date <= date_part('EPOCH_SECOND', to_timestamp(current_date)) 
    and closed_indicator 
    qualify max_end_date=end_date and min_start_date=start_date
),
g as (
    select upk_key2, 
        arrayagg(object_construct('start', start_date, 'end', end_date)) as ranges 
        from b
        group by upk_key2
), 
non_overlap as (
    select upk_key2, ranges, SPLIT_RANGES(ranges) as clean_ranges from g
)
select upk_key2, 
    dateadd(day, 1, to_date(to_timestamp(clean_range.value:start))) as start_date,
    dateadd(day, 1, to_date(to_timestamp(clean_range.value:end))) as end_date
from non_overlap, lateral flatten(input => clean_ranges) as clean_range