# Utility functions

def get_dist_cts(var, table, filter = 'TRUE', filter_print = None, group_by_list = None):
    '''
    return a sql string to summarize a continuous variable
    '''
    if filter_print is None:
        filter_print = filter
    group_by_str = ''
    order_by_str = ''
    group_by_select_str = ''
    if group_by_list is not None:
        group_by = ",".join(group_by_list)
        group_by_select_str = group_by + ','
        group_by_str = 'group by ' + group_by
        order_by_str = 'order by ' + group_by
    return f"""
        select 
            {group_by_select_str}
            '{var}' as var, 
            '{filter_print}' as filter,
            count(*) as n,
            count_if({var} is null) as n_null,
            count_if({var} < 0) as n_neg,
            count_if({var} = 0) as n_zero,
            count_if({var} > 0) as n_pos,
            avg({var}) as mean,
            variance({var}) as variance,
            min({var}) as min,
            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY {var}) as p_01,
            PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY {var}) as p_05,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY {var}) as p_10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {var}) as p_25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {var}) as median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {var}) as p_75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY {var}) as p_90,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {var}) as p_95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {var}) as p_99,
            max({var}) as max
        from {table}
        where {filter}
        {group_by_str}
        {order_by_str}
    """

def get_ce(bene_input = None, mx_version = None, grace_period = 45):
    '''
    returns a sql string to get continous enrollment table
    params:
        bene_input takes a subset of BENEFICIARY_* table from any MX_counter schema. If
        not provided, will use the BENEFICIARY_LS_GA table for the corresponding mx_version.
        grace_period defines the minimal allowable gaps between two enrollment windows still
        be considered 'continuous'.
    '''
    if bene_input is None and mx_version is None:
        print("Need either bene_input or mx_version!")
        raise
    if bene_input is None and mx_version is not None:
        bene_input = f"MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.BENEFICIARY_LS_GA"
    elif bene_input is not None and mx_version is not None:
        bene_input = f"""
        (
            select * from MAP_ENCOUNTERS.MX_ENCOUNTERS_{mx_version}.BENEFICIARY_LS_GA
            where upk_key2 in (select distinct upk_key2 from {bene_input})
        )
        """
        
    sql_split = get_sql_split_fun(grace_period = grace_period)
    sql_ce_mx = get_sql_ce_by_type_kh(bene_input = bene_input, type = "mx", grace_period = grace_period)
    sql_ce_rx = get_sql_ce_by_type_kh(bene_input = bene_input, type = "rx", grace_period = grace_period)

    sql_ce = f"""
        with mx as ({sql_ce_mx}), rx as ({sql_ce_rx})
        select mx.upk_key2, 
            greatest(mx.start_date, rx.start_date) as start_date,
            least(mx.end_date, rx.end_date) as end_date
            from mx inner join rx
            on mx.upk_key2 = rx.upk_key2
            where mx.start_date <= rx.end_date and mx.end_date >= rx.start_date
        ;
    """
    return sql_ce
    
def get_sql_split_fun(grace_period = 45):
    sql_split = f"""
        create or replace function SPLIT_RANGES(dates variant)
           returns variant
           language javascript
        as '
        return DATES
                    .sort(function (a, b) {{ return a.start - b.start || a.end - b.end; }})
                    .reduce(function (r, a) {{
                          var last = r[r.length - 1] || [];
                          if (last.start <= a.start && a.start <= last.end + {grace_period}*24*60*60) {{
                                if (last.end < a.end) {{
                                      last.end = a.end;
                                }}
                                return r;
                          }}
                          return r.concat(a);
                    }}, []);
        '
        ;
        """
    return sql_split

def get_sql_ce_by_type_kh(bene_input, type, grace_period = 45):
    if type == 'mx':
        cov_ind = 'MEDICAL_COVERAGE_INDICATOR'
    elif type == 'rx':
        cov_ind = 'PHARMACY_COVERAGE_INDICATOR'

    sql_ce = f"""    
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
    """
    return sql_ce
