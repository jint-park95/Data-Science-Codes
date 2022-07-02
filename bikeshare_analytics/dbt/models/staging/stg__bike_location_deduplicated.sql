

WITH

bike_location AS (

    select * from {{ ref('base__bike_location') }}

),

non_zero_loc AS (

    SELECT *
    FROM bike_location
    WHERE 
        lon != 0 
        AND lat != 0

),

deduplicated AS (

    select

        last_updated_ct,
        bike_id,
        lat,
        lon,
        is_reserved,
        is_disabled,
        pricing_plan_id

    from non_zero_loc
    GROUP BY 1,2,3,4,5,6,7

),

add_loc AS (

    select

        *,
        ST_GEOGPOINT(lon, lat) AS bike_geo_loc
        
    from deduplicated

),

add_lag_diff AS (

    select

        *,
        
        /* Calculate the difference in last_updated_ct in seconds since the most recent update */
        datetime_diff(
            last_updated_ct, 
            lag(last_updated_ct, 1) over(partition by bike_id order by last_updated_ct), 
            SECOND
        ) AS time_diff_in_seconds,

        /* Calculate the distance traveled between since the most recent update */
        ST_DISTANCE(
            bike_geo_loc, 
            LAG(bike_geo_loc, 1) over(partition by bike_id order by last_updated_ct)
        ) AS distance_in_meter
        
    from add_loc   

),

filter_distance AS (

    select

        * except(distance_in_meter),
        
        /* Overwrite distance travled column with 0 for records: 
        - not updated for 5+ minutes 
        - exceeding approx. speed of 200km/hour
        */
        CASE 
            WHEN time_diff_in_seconds > 300 THEN 0
            WHEN SAFE_DIVIDE(distance_in_meter, COALESCE(time_diff_in_seconds)) > 200*1000/3600 THEN 0
            ELSE distance_in_meter
        END AS distance_in_meter


    from add_lag_diff
)

select * from filter_distance