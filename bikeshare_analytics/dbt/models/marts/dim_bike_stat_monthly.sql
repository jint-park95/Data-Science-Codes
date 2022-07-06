with 

trip as (

    select
     
        format_datetime("%Y%m", trip_begin_ct) as trip_begin_mnth,
        format_datetime("%Y%m", trip_end_ct) as trip_end_mnth,
        * 

    from {{ ref('fct_bike_trip') }}

),

trip_agg as (

    select

        trip_begin_mnth,
        bike_id,
        COUNT(bike_id) as trip_count_total,
        SUM(trip_duration_second) as trip_duration_second_total,
        SUM(distance_meter) as trip_distance_meter_total
    
    from trip

    group by 1,2

),

bike_location as (

    select 
    
        * 
    
    from {{ ref('stg__bike_location_deduplicated') }}

),

bike_month_distinct as (

    select
        
        format_datetime("%Y%m", last_updated_ct) as mnth,
        bike_id

    from bike_location
    
    group by 1,2

),

bike_monthly_join as (

    select

        bike_month_distinct.mnth,
        bike_month_distinct.bike_id,

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

    from bike_month_distinct

    left join trip_agg ON 

        bike_month_distinct.mnth = trip_agg.trip_begin_mnth
        AND bike_month_distinct.bike_id = trip_agg.bike_id

)

select * from bike_monthly_join
order by mnth, trip_distance_meter_total desc


