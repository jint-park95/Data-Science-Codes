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

        rank() 
            over(
                partition by bike_id 
                order by last_updated_ct
            ) 
        as mvmt_begin_rank

    from bike_location
    where is_start_movement is true

),

bike_movement_order_joined as (

    select 

        bike_location.*,

        /* assign trip value to location timestamp to be used to group individual trips */
        case
            when is_start_movement is true then mvmt_begin_rank
            when 
                distance_in_meter > 0 
                or is_end_movement is true 
                then 
                    max(mvmt_begin_rank) 
                        over(
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

        bike_id,
        bike_trip_rank,
        min(last_updated_ct) as trip_begin_ct,
        max(last_updated_ct) as trip_end_ct,

        datetime_diff(
            max(last_updated_ct), 
            min(last_updated_ct), 
            second
        ) as trip_duration_second,
        
        sum(distance_in_meter) as distance_meter,

        safe_divide(
            sum(distance_in_meter), 
            datetime_diff(max(last_updated_ct), 
            min(last_updated_ct), 
            second)
        ) as meters_per_second

    from bike_movement_order_joined

    where bike_trip_rank is not null

    group by 
        bike_id, 
        bike_trip_rank

)

select * from group_bike_trip