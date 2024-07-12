# ETL pipeline
![Untitled design (1)](https://github.com/user-attachments/assets/1d5e3f7b-51a3-4fae-b0e2-acf0b8cd41e2)

## Overview

This ETL (Extract, Transform, Load) pipeline retrieves data from the SpaceX API, transforms the data into three separate tables, and loads these tables into a PostgreSQL database. The pipeline is orchestrated using Apache Airflow.

## Pipeline Components

- Extract: Fetches data from the SpaceX API and stores it using Airflow's XCom.
- Transform: Normalizes the JSON data into three separate pandas DataFrames and saves them as CSV files.
- Load: Creates tables in the PostgreSQL database and loads the transformed data from the CSV files into these tables.
