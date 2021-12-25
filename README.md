# BTC Volatility Extractor ETL

## Description

This repository contains an ETL application to extract bitcoin volatility information.
The application fetch the source data set from remote URL and persists the extracted information in a PostgreSQL database. 

The application will populate 02 tables,
- **stg_table** - contains the data from the json as it is. Can be used as a staging table for other ETLs, if any.
- **volatility_table** - contains a daily summary of stg_table including the daily volatility.

Before writing data to the tables, it will remove any rows that belong to the same time period to avoid records being duplicated by multiple executions.

### Considerations

- Data set is fairly small, (i.e 87840 records/year, 33M) also it is a one time job, hence big data ETL solution is not required.
- Performance considerations were ignored during reading/processing/persisting.
- `price_open` was considered to derive the volatility.
- any time intervals with 0 trades were ignored for the volatility calculation.


## How to Run

### Prerequisites

- Python3 installed.
- PostgreSQL installed and configured.

### Install Dependencies

I have used `pandas`, `requests_cache`, `psycopg2` external libraries. Following is a sample script to install them
using pip.

```
# install pip
sudo apt install python3-pip

pip3 install pandas
pip3 install requests_cache
pip3 install sqlalchemy

# postgresql driver
# for ubuntu we need libpq-dev installed for psycopg2
sudo apt-get install libpq-dev
pip3 install psycopg2

```

### Database Setup

Refer the [DDL script](/sql_ddl/ddl.sql) to populate the sql tables.

### Run Application

- Define application configuration with environment variables.
  
  ```
  export BTC_ETL_SRC_FILE_URL=<public_access_url>
  export BTC_ETL_DB_CON=postgresql://<user>:<pwd>@<host>:<port>/<database>

  export BTC_ETL_STG_TABLE_SCHEMA=<table_schema_for_source_dataset>
  export BTC_ETL_STG_TABLE_NAME=<table_name_for_source_dataset>
  
  export BTC_ETL_VOLATILITY_TABLE_SCHEMA=<table_schema_daily_volatility_table>
  export BTC_ETL_VOLATILITY_TABLE_NAME=<table_name_daily_volatility_table>
  
  ```

- Execute the application.
    ```
    python3 src/main.py
    ```

## Deployment Plan

[Deployment Plan](DeploymentPlan.md)
