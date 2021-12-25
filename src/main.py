import os
import logging
from btc import VolatilityETL


def main():

    logging.basicConfig(level = logging.INFO)

    src_file_url = os.getenv("BTC_ETL_SRC_FILE_URL")
    con_str = os.getenv("BTC_ETL_DB_CON")

    stg_table_schema = os.getenv("BTC_ETL_STG_TABLE_SCHEMA")
    stg_table_name = os.getenv("BTC_ETL_STG_TABLE_NAME")
    v_table_schema = os.getenv("BTC_ETL_VOLATILITY_TABLE_SCHEMA")
    v_table_name = os.getenv("BTC_ETL_VOLATILITY_TABLE_NAME")

    etl = VolatilityETL()
    etl.process_file(src_file_url)
    etl.update_db(con_str, stg_table_schema, stg_table_name, v_table_schema, v_table_name)


if __name__ == "__main__":
    main()
