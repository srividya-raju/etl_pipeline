select
    *
from
    {{ref('reporting_table')}}
where
    date(order_placed_date) > CURRENT_DATE()
    or date(order_placed_date) < date('2020-01-01')