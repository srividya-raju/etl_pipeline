{% macro dedupe(model, partition_cols) %}
    with deduped as (
        select
            *,
            row_number() over (
                partition by {{ partition_cols | join(', ') }}
                order by {{ partition_cols | join(', ') }}
            ) as rn
        from {{ ref(model) }}
    )
    select *
    from deduped
    where rn = 1
{% endmacro %}
