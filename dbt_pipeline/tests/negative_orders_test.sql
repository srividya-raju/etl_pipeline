
    select *
    from {{ref('Reporting_table')}}
    where count_orders<0