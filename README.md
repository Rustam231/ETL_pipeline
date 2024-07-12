# ETL pipeline
![Untitled design](https://github.com/user-attachments/assets/ab81f89c-cc91-40c0-be92-d29ab007884a)

## Overview

This ETL (Extract, Transform, Load) pipeline retrieves data from the SpaceX API, transforms the data into three separate tables, and loads these tables into a PostgreSQL database. The pipeline is orchestrated using Apache Airflow.

## Pipeline Components

- Extract: Fetches data from the SpaceX API and stores it using Airflow's XCom.
- Transform: Normalizes the JSON data into three separate pandas DataFrames and saves them as CSV files.
- Load: Creates tables in the PostgreSQL database and loads the transformed data from the CSV files into these tables.

## DAG Tasks
- Extract Task: Downloads data from the SpaceX API.
- Transform Task: Converts JSON data into three distinct CSV files.
- Create Table Tasks: Creates PostgreSQL tables to store the transformed data.
- Load Tasks: Loads the data from the CSV files into the PostgreSQL tables.
