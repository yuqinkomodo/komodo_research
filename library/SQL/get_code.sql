-- get_code
-- param code_file, pattern_list_str
select *
from {code_file}
where {search_var} ilike any ({pattern_str})