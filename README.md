# Data Pipeline Project with Airflow and Redshift

This repository contains the template and implementation for a data pipeline project using Apache Airflow and Amazon Redshift. It is part of a Udacity course, and it is designed to demonstrate the ability to build, schedule, and monitor data pipelines in a cloud-based environment.

## Project Overview

The project builds an ETL data pipeline with Airflow that loads and transforms data in Redshift. The pipeline includes custom operators to stage data from S3, load fact and dimension tables, and perform data quality checks.

You can run the pipeline in the Udacity workspace or locally by cloning this repository.

## DAG Overview

The Airflow DAG contains the following tasks:

Begin_execution
├── Stage_events
├── Stage_songs
↓
Load_songplays_fact_table
├── Load_user_dim_table
├── Load_song_dim_table
├── Load_artist_dim_table
└── Load_time_dim_table
↓
Run_data_quality_checks
↓
Stop_execution


### DAG Configuration

The DAG should be configured with the following parameters:

- `depends_on_past=False`
- `retries=3`
- `retry_delay=5 minutes`
- `catchup=False`
- `email_on_retry=False`

## Operators to Implement

### 1. `StageToRedshiftOperator`

- Loads JSON-formatted files from S3 to Redshift using the `COPY` command.
- Supports templated S3 keys for time-based file partitioning.
- Parameters:
  - `table`
  - `s3_bucket`
  - `s3_key`
  - `json_format`
  - `redshift_conn_id`
  - `aws_credentials_id`

### 2. `LoadFactOperator`

- Loads data into the fact table using a provided SQL query.
- Typically supports `INSERT` operations.

### 3. `LoadDimensionOperator`

- Loads data into dimension tables using `TRUNCATE-INSERT` or `APPEND` mode.
- Parameters:
  - `table`
  - `sql_query`
  - `mode`: `'truncate-insert'` or `'append'`

### 4. `DataQualityOperator`

- Executes one or more data quality checks.
- Each check consists of a SQL query and an expected result.
- If actual and expected results do not match, the operator raises an exception.

## Getting Started

To get started:

- **Workspace Option**: Work on the Udacity provided workspace and submit through the platform.
- **Local Option**:
  1. Clone this repo:  
     ```bash
     git clone <repository-url>
     ```
  2. Set up Airflow and dependencies locally.
  3. Run your DAG using the Airflow UI.
  4. Submit either your forked GitHub repo or a zip file of your final solution.

## Final Notes

- All SQL interactions target Redshift.
- Focus on reusable, parameterized operators.
- Make use of Airflow's hooks and connections whenever possible.

---

**License**: For educational purposes only.

**Author**: Esther Lopez


