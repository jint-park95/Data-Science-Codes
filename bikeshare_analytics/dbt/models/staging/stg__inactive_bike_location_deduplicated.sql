{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'last_updated_ct', 'data_type': 'timestamp'},
    partitions = partitions_to_replace,
    cluster_by = 'bike_id'
  )
}}

WITH

bike_location as (

    select * from {{ ref('base__inactive_bike_location') }}
    {% if is_incremental() %}
    where date(last_updated_ct) in ( {{ partitions_to_replace | join(',') }} )
    {% endif %}

),

non_zero_loc as (

    select *
    from bike_location
    where 
        lon != 0 
        AND lat != 0

),

deduplicated AS (

    select

        last_updated_ct,
        bike_id,
        vehicle_type_id,
        lat,
        lon,
        is_reserved,
        is_disabled,
        pricing_plan_id

    from non_zero_loc
    GROUP BY 1,2,3,4,5,6,7,8

),

add_loc as (

    select

        *,
        ST_GEOGPOINT(lon, lat) as bike_geo_loc
        
    from deduplicated

),

add_lag_diff as (

    select

        *,
        
        /* Calculate the difference in last_updated_ct in seconds since the most recent update */
        datetime_diff(
            last_updated_ct, 
            lag(last_updated_ct, 1) over(partition by bike_id order by last_updated_ct), 
            second
        ) as time_diff_in_seconds,

        /* Calculate the distance traveled between since the most recent update */
        st_distance(
            bike_geo_loc, 
            LAG(bike_geo_loc, 1) over(partition by bike_id order by last_updated_ct)
        ) as distance_in_meter
        
    from add_loc   

),

filter_distance as (

    select

        * except(distance_in_meter),
        
        /* Overwrite distance travled column with 0 for records: 
        - not updated for 5+ minutes 
        - exceeding approx. speed of 200km/hour
        */
        case
            when time_diff_in_seconds > 300 then 0
            when SAFE_DIVIDE(distance_in_meter, COALESCE(time_diff_in_seconds)) > 200*1000/3600 then 0
            else distance_in_meter
        end as distance_in_meter


    from add_lag_diff

),

movement_boolean as (

    select

        *,
        case 
            when lead(distance_in_meter, 1) over(partition by bike_id order by last_updated_ct) > 0 and distance_in_meter = 0 then true
            else false
        end as is_start_movement,

        case
            when lead(distance_in_meter, 1) over(partition by bike_id order by last_updated_ct) = 0 and distance_in_meter > 0 then true
            else false
        end as is_end_movement,

    
    from filter_distance 


),


extract_datetime as (

    select 

        extract(year from last_updated_ct) as year_int,
        extract(month from last_updated_ct) as month_int,
        extract(day from last_updated_ct) as day_int,
        *
    
    from movement_boolean

),

add_new_date_indicator as (

    select

        *,

        case 
            when day_int != lag(day_int, 1) over(partition by bike_id order by last_updated_ct) then true 
            else false
        end as is_new_day_ct,

        case 
            when month_int != lag(month_int, 1) over(partition by bike_id order by last_updated_ct) then true 
            else false
        end as is_new_month_ct

    from extract_datetime

)

select * from add_new_date_indicator