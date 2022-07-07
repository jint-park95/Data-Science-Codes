with 

trip as (

    select
     
        datetime_trunc(trip_begin_ct, day) as trip_begin_day,
        datetime_trunc(trip_begin_ct, day) as trip_end_day,
        * 

    from {{ ref('fct_bike_trip') }}

),

trip_agg as (

    select

        trip_begin_day,
        bike_id,
        count(bike_id) as trip_count_total,
        sum(trip_duration_second) as trip_duration_second_total,
        sum(distance_meter) as trip_distance_meter_total
    
    from trip

    group by 1,2

),

bike_location as (

    select 
    
        * 
    
    from {{ ref('stg__bike_location_deduplicated') }}

),

bike_location_agg as (

    select
        
        datetime_trunc(last_updated_ct, day) as trip_begin_day,
        bike_id,
        count(bike_id) as location_update_count,
        count(bike_id) / 1440 * 100 as location_uptime_perc,
        sum(time_diff_in_seconds) as move_duration_second_total,
        sum(distance_in_meter) as move_distance_meter_total

    from bike_location
    
    group by 1,2

),

bike_monthly_join as (

    select

        bike_location_agg.trip_begin_day,
        bike_location_agg.bike_id,

        coalesce(location_update_count, 0) as location_update_count,
        coalesce(location_uptime_perc, 0) as location_uptime_perc,

        coalesce(bike_location_agg.move_duration_second_total, 0) as move_duration_second_total,
        coalesce(bike_location_agg.move_distance_meter_total, 0) as move_distance_meter_total,

        coalesce(trip_agg.trip_count_total, 0) as trip_count_total,
        coalesce(trip_agg.trip_duration_second_total, 0) as trip_duration_second_total,
        coalesce(trip_agg.trip_distance_meter_total, 0) as trip_distance_meter_total,

        coalesce(
            safe_divide(
                trip_agg.trip_distance_meter_total, 
                trip_agg.trip_count_total)
            , 0) 
        as trip_average_distance_meter_per_trip,

        coalesce(
            safe_divide(
                trip_agg.trip_distance_meter_total, 
                trip_agg.trip_duration_second_total)
            , 0
        ) as trip_average_speed_meter_per_second

    from bike_location_agg

    left join trip_agg  
        using (trip_begin_day, bike_id)

)

select * from bike_monthly_join
