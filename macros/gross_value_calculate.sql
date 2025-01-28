{% macro calculate_gross_value(table, group_by_column) %}
    select 
        {{ group_by_column }} as group_key,
        sum(f.gross_value) as total_sales_value
    from {{ ref(table) }} f  
    join {{ ref('dim_dates') }} d 
        on f.order_date = d.date  
    group by {{ group_by_column }}
{% endmacro %}