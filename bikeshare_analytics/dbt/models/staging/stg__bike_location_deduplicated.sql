

WITH

bike_location AS (

    SELECT * FROM `jintaepark-portfolio-project`.`bikeshare_analytics_dev`.`base__bike_location`

),

deduplicated AS (

    SELECT

        last_updated_ct,
        bike_id,
        lat,
        lon,
        is_reserved,
        is_disabled,
        pricing_plan_id

    FROM bike_location 
    GROUP BY 1,2,3,4,5,6,7

),

add_loc AS (

    SELECT

        *,
        ST_GEOGPOINT(lon, lat) AS bike_geo_loc
        
    FROM deduplicated

),

add_lag_diff AS (

    SELECT

        *,
        
        /* Calculate the difference in last_updated_ct in seconds since the most recent update */
        DATETIME_DIFF(
            last_updated_ct, 
            LAG(last_updated_ct, 1) OVER(PARTITION BY bike_id ORDER BY last_updated_ct), 
            SECOND
        ) AS time_diff_in_seconds,

        /* Calculate the distance traveled between since the most recent update */
        ST_DISTANCE(
            bike_geo_loc, 
            LAG(bike_geo_loc, 1) OVER(PARTITION BY bike_id ORDER BY last_updated_ct)
        ) AS distance_in_meter
        
    FROM add_loc   

),

filter_distance AS (

    SELECT

        * EXCEPT(distance_in_meter),
        
        /* Overwrite distance travled column with 0 for records not updated for 5+ minutes */
        CASE 
            WHEN time_diff_in_seconds > 300 THEN 0
            ELSE distance_in_meter
        END AS distance_in_meter


    FROM add_lag_diff
)

SELECT * FROM filter_distance