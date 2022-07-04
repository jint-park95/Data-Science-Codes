{%- macro dev_data_limit(datetime_column, increment='day', increment_value=3) -%}
{% if target.name == 'dev' and increment in ['day', 'month', 'quarter', 'year']-%}
where {{ datetime_column }} >= date_sub( current_datetime(), interval {{ increment_value }} {{ increment }})
{%- endif %}
{%- endmacro -%}

select
    *
from {{ ref('base__bike_location') }}
{{ dev_data_limit(datetime_column = 'last_updated_ct', increment = 'day', increment_value = 2) }}