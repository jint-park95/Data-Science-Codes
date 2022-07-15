### Project Overview

#### Background:

- In this project, I will create a small-scale Extract-Load-Transform pipeline using location data of unoccpuied bikeshare coming from [General Bikeshare Feed Specification](https://github.com/NABSA/gbfs)
- Goal of this portfolio is to surface and report on unoccupied movement bike movements to be able to validate any suspicious movements or transportating efforts un-associated with active users 

#### This project is consisted in three parts:

- extract/load: python script to extract / transform / load bikeshare data to BigQuery
- transform: dbt project to ingest / transform / surface business-relevant datasets
- report: present data & visualization that is relevant for ficticious business stakeholder by leveraging transformed data & jupiter notebook (python)

### E/L

- Source of data comes from a intiative for standardized data feed for shared mobility system availability ([Link](https://github.com/NABSA/gbfs))
- Custom python script pulls location data for currently unoccpied bicycle location from a company named Austin B-cycle
- Ran in scheduler, but written as functions to be easily converted as callables if converted to run in airflow

### Analytics Pipeline

#### Pipeline DAG:
![image](https://user-images.githubusercontent.com/52013434/179142974-2ca1bdea-73a6-4cd6-892e-1a1fc0742e72.png)

source: 

- `raw_bikeshare_location` in DAG
- destination for aforementioned Python E/L

base-level:
- `base__bike_location` in DAG ([link](https://github.com/jint-park95/Data-Science-Codes/blob/main/bikeshare_analytics/dbt/models/base/base__bike_location.sql)) 
- Focused on renaming and recasting 
- 1:1 relationship with source table for clarity

staging-level: 
- `stg__bike_location_deduplicated` ([link](https://github.com/jint-park95/Data-Science-Codes/blob/main/bikeshare_analytics/dbt/models/staging/stg__bike_location_deduplicated.sql)) 
- With DBT scheduler running daily, this table is configured to insert & overwrite previous 2 days worth of raw location data
  - If configured for a real business, this would be added with a recurring `--full-refresh` run once a week to ensure data accuracy
- This layer also deduplicates any location data (in case of E/L issues), add business logic(s) to filter out obvious GPS errors and label consecutive movements to be later grouped

marts: 
- `fct_bike_trip` ([link](https://github.com/jint-park95/Data-Science-Codes/blob/main/bikeshare_analytics/dbt/models/marts/fct_bike_trip.sql)) 
- Aggregate any consecutive movement as “trips”
- This layer would be an example of a BI layer - one most likely as a table with self-servicing potentials

reports: 
- `dim_bike_stat_daily` ([link](https://github.com/jint-park95/Data-Science-Codes/blob/main/bikeshare_analytics/dbt/models/marts/dim_bike_stat_daily.sql)) 
- Aggregate day-over-day total of unoccpuied movement
- This layer would be an example of a BI layer - one most likely as a table for specific reporting function

### Report

- Data visuliazation represented as exported Jupiter Notebook
- Can easily be subsituted as BI layer