{{
    config(
        materialized='table'
    )
}}

with

bike_location as (

    select * from {{ ref('stg__bike_location_deduplicated') }}

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
        mvmt_begin_rank,
        case
            when is_end_movement is true then null
            when is_start_movement is true then mvmt_begin_rank
            when distance_in_meter > 0 then 
                max(mvmt_begin_rank) over(
                    partition by bike_id 
                    order by last_updated_ct 
                    rows between unbounded preceding and current_row
                )
            else null
        end as bike_trip_rank

    from bike_location

    left join trip_start_order
        using(last_updated_ct, bike_id)

)


select * from bike_movement_order_joined
WHERE 
  bike_id ="21ad824e-ee5e-4244-9227-e7cc6af85328"
  AND last_updated_ct <= ("2022-07-02T18:40:26")
order by last_updated_ct desc
