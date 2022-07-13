### Project Overview

This project is consisted in three parts:

- extract/load: python script to extract / transform / load bikeshare data to BigQuery
- transform: dbt project to ingest / transform / surface business-relevant datasets
- report: jupiter notebook (python) for data visualization

Goal:

- Surface and report on unoccupied movement bike movements to be able to validate:
    - Suspicious movements
    - Transporting bike from station to station
    - etc.

### E/L

- Source of data comes from a intiative for standardized data feed for shared mobility system availability ([Link](https://github.com/NABSA/gbfs))
- Custom python script pulls location data for currently unoccpied bicycle location from a company named Austin B-cycle
- Ran in scheduler, but written as functions to be easily converted as callables if converted to run in airflow

### Analytics Pipeline

- base level - renaming and recasting. 1:1 with source table
- staging - incrementally ingest raw location data / deduplicate / add business logic(s) for obvious GPS errors and consecutive movements
- marts - Aggregate any consecutive movement as “trips” / Designed as a layer to be digested in BI layer (if exists)
- reports - Aggregate day-over-day total of unoccpuied movement / Designed as a layer to be digested in BI layer (if exists)

### Report

- Data visuliazation represented as exported Jupiter Notebook
- Can easily be subsituted as BI layer
