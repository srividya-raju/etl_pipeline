select
    *
from
    {{ref('reporting_table')}}
where
    avg_rating < 0
    or avg_rating > 5