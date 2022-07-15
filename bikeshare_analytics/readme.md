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

- source: `raw_bikeshare_location` (link) - destination for Python E/L
- base: `base__bike_location` (link) - renaming and recasting. 1:1 with source table
- staging: `stg__bike_location_deduplicated` (link) - incrementally ingest raw location data / deduplicate / add business logic(s) for obvious GPS errors and label consecutive movements
- marts: `fct_bike_trip` (link) - Aggregate any consecutive movement as “trips” / Designed as a layer to be digested in BI layer (if exists)
- reports: `dim_bike_stat_daily` (link) - Aggregate day-over-day total of unoccpuied movement / Designed as a layer to be digested in BI layer (if exists)

### Report

- Data visuliazation represented as exported Jupiter Notebook
- Can easily be subsituted as BI layer
