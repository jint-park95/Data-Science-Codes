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
        CAST(lon AS DECIMAL) AS lon,
        CAST(lat AS DECIMAL) AS lat,
        is_reserved,
        is_disabled,
        pricing_plan_id,
        CAST(last_updated AS DATETIME) as last_updated_ct

    from source

)

select * from renamed
