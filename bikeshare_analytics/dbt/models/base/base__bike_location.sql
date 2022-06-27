with 

source as (

    select
        *
    FROM {{ source('bikeshare_analytics_etl', 'raw__bikeshare_location') }}

),

renamed as (

    select

        bike_id,
        vehicle_type_id,
        lat,
        lon,
        is_reserved,
        is_disabled,
        pricing_plan_id,
        last_updated as last_updated_ct

    from source

)

select * from renamed
