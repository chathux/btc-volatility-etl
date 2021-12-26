import logging
from io import StringIO

import pandas as pd
import requests
import requests_cache
from sqlalchemy import create_engine


class VolatilityETL:
    __df = None
    __v_df = None
    __logger = None

    def __init__(self):
        self.__logger = logging.getLogger('etl')
        self.__logger.setLevel(logging.INFO)
        requests_cache.install_cache()

    # reads the json file from remote url and process it to retrieve volatility information
    # and stores as dataframes
    def process_file(self, url):
        response = requests.get(url)
        content = response.content.decode()
        df = pd.read_json(StringIO(content))

        # update df with extra column for the volatility date
        df["volatility_date"] = df["time_period_start"].str[:10]

        zero_trade_intervals = df[df.trades_count == 0].shape[0]
        if zero_trade_intervals > 0:
            self.__logger.warning("skipping {} records with zero trades".format(zero_trade_intervals))

        df_cleansed = df[df.trades_count > 0]

        v_df = df_cleansed.groupby(['volatility_date']).agg(
            volatility=('price_open', 'std'),
            price_open=('price_open', 'first'),
            price_close=('price_close', 'last'),
            price_low=('price_low', 'min'),
            price_high=('price_high', 'max'),
            trades_count=('trades_count', 'sum'),
            sample_count=('trades_count', 'count'),
        ).reset_index().rename(columns={'volatility_date': 'date'})

        self.__df = df
        self.__v_df = v_df

    def update_db(self, con_str, src_schema, src_table, v_table_schema, v_table):
        assert self.__df is not None, "source data frame are not initialized !!!"
        assert self.__v_df is not None, "volatility data frame are not initialized !!!"

        db = create_engine(con_str)
        conn = db.connect()

        self.__logger.info("persisting source data")
        self.__write_to_table(conn, src_schema, src_table, self.__df[self.__df.columns.drop('volatility_date')],
                              "delete from {}.{} where time_period_start between '{}' and '{}'".format(
                                  src_schema, src_table,
                                  self.__df.iloc[0]["time_period_start"],
                                  self.__df.iloc[-1]["time_period_start"]
                              )
                              )

        self.__logger.info("persisting volatility data")
        self.__write_to_table(conn, v_table_schema, v_table, self.__v_df,
                              "delete from {}.{} where date between '{}' and '{}'".format(
                                  v_table_schema, v_table,
                                  self.__v_df.iloc[0]["date"],
                                  self.__v_df.iloc[-1]["date"]
                              )
                              )
        conn.close()

    def __write_to_table(self, conn, table_schema, table_name, df, delete_query):
        # remove existing data for the current date set date range, to avoid duplication
        delete_result = conn.execute(delete_query)
        self.__logger.info("removed {} records from {}".format(delete_result.rowcount, table_name))

        df.to_sql(table_name, conn, schema=table_schema, if_exists="append", index=False)
        self.__logger.info("successfully updated table with {} records".format(df.shape[0]))
