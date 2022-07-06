with 

source as (

    select
        *
    from {{ source('bikeshare_analytics_etl', 'raw__bikeshare_location') }}

),

renamed as (

    select

        bike_id,
        vehicle_type_id,
        cast(lon as decimal) AS lon,
        cast(lat as decimal) AS lat,
        is_reserved,
        is_disabled,
        pricing_plan_id,
        datetime(cast(last_updated AS timestamp), "America/Chicago") AS last_updated_ct

    from source

)

select * from renamed
{{ dev_data_limit(datetime_column = 'last_updated_ct', increment = 'day', increment_value = 5) }}
