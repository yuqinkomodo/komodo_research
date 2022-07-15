-- udf split_ranges
-- param: grace_period
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