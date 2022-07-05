{{
    config(
        materialized='table'
    )
}}

with

bike_location as (

    select 

        last_updated_ct,
        bike_id,
        lon,
        lat,
        bike_geo_loc,
        time_diff_in_seconds,
        distance_in_meter,
        is_start_movement,
        is_end_movement

    from {{ ref('stg__bike_location_deduplicated') }}

),

movement_start_order as (

    select
        last_updated_ct,
        bike_id,
        rank() over (partition by bike_id order by last_updated_ct) as mvmt_begin_rank
    from bike_location
    where is_start_movement is true

),

bike_movement_order_joined as (

    select 

        bike_location.*,
        case
            when is_end_movement is true then null
            when is_start_movement is true then mvmt_begin_rank
            when distance_in_meter > 0 then 
                max(mvmt_begin_rank) over(
                    partition by bike_id 
                    order by last_updated_ct 
                    rows between unbounded preceding and current row
                )
            else null
        end as bike_trip_rank

    from bike_location

    left join movement_start_order
        using(last_updated_ct, bike_id)

),

group_bike_trip as (

    select

        *

    from bike_movement_order_joined

)


select * from group_bike_trip

