# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging

# COMMAND ----------

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class Dim:
    def __init__(self, name, columns):
        self.name = name.lower()  # Normalize to lowercase for consistency
        self.silver_path = "abfss://silver@storagecarproject.dfs.core.windows.net/carsales"
        self.gold_dim_path = f"abfss://gold@storagecarproject.dfs.core.windows.net/dim_{self.name}"
        self.table_name = f"car_catalog.gold.dim_{self.name}"  # Unified table name
        self.columns = ", ".join(columns)
        self.column_list = columns
        self.spark = SparkSession.builder.getOrCreate()  # Ensure spark is available
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def source_data(self):
        try:
            df = self.spark.sql(f"SELECT DISTINCT {self.columns} FROM PARQUET.`{self.silver_path}`").alias("src")
            return df
        except Exception as e:
            self.logger.error(f"Source read failed: {e}")
            raise

    def is_incremental(self):
        return self.spark.catalog.tableExists(self.table_name)

    def sink_data(self):
        if self.is_incremental():
            df_sink = self.spark.sql(f"SELECT * FROM {self.table_name}").alias("sink")
            return df_sink
        else:
            query = f"SELECT {self.columns} FROM PARQUET.`{self.silver_path}` WHERE 1=0"
            df_sink = self.spark.sql(query).alias("sink")
            return df_sink

    def old_data(self):
        src_df = self.source_data()
        sink_df = self.sink_data()
        df_old = src_df.join(
            sink_df,
            src_df[f"{self.name}_id"] == sink_df[f"{self.name}_id"],
            how="left"
        ).filter(col(f"sink.dim_{self.name}_key").isNotNull()) \
         .select([col(f"src.{c}") for c in self.column_list] + [col(f"sink.dim_{self.name}_key")])
        return df_old

    def new_data(self):
        src_df = self.source_data()
        sink_df = self.sink_data()
        df_new = src_df.join(
            sink_df.select(f"{self.name}_id"),
            on=[f"{self.name}_id"],
            how="left_anti"
        ).select([col(f"src.{c}") for c in self.column_list])
        return df_new

    def final_data(self):
        df_new = self.new_data()
        if df_new.count() == 0:
            self.logger.info(f"No new data found for dim_{self.name}")
            return None
        
        if self.is_incremental():
            df_old = self.old_data()
            # Ensure columns align for union
            df_final = df_new.union(df_old.select(self.column_list))
        else:
            df_final = df_new

        return df_final

    def create_surrogate_key(self, df):
        window_spec = Window.orderBy(f"{self.name}_id")
        df_with_key = df.withColumn(f"dim_{self.name}_key", row_number().over(window_spec))
        return df_with_key

    def write_to_deltalake(self, df):
        try:
            if df is None:
                self.logger.info(f"Skipping write for dim_{self.name} - no data to process")
                return

            df_final_with_key = self.create_surrogate_key(df)
            if self.is_incremental():
                delta_table = DeltaTable.forPath(self.spark, self.gold_dim_path)
                merge_condition = f"target.{self.name}_id = source.{self.name}_id"
                delta_table.alias("target").merge(
                    df_final_with_key.alias("source"),
                    merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                self.logger.info(f"Incremental update completed for {self.gold_dim_path}")
            else:
                existing_tables = self.spark.sql("SHOW TABLES IN car_catalog.gold").filter(
                    col("tableName").like(f"dim_{self.name}")
                ).collect()
                if existing_tables:
                    raise ValueError(f"Location {self.gold_dim_path} is already tied to {existing_tables[0]['tableName']}")

                df_final_with_key.write.format("delta").mode("overwrite").save(self.gold_dim_path)
                self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name}
                    USING DELTA
                    LOCATION '{self.gold_dim_path}'
                """)
                self.logger.info(f"Initial load completed for {self.gold_dim_path}")
            
            display(df_final_with_key)

        except Exception as e:
            self.logger.error(f"Write failed: {e}")
            raise

    def run(self):
        self.logger.info(f"Starting pipeline for dim_{self.name}")
        df_src = self.source_data()
        self.logger.info(f"Source data extracted: {df_src.count()} rows")
        
        df_final = self.final_data()
        if df_final is None:
            self.logger.info(f"Pipeline stopped - no new data to process for dim_{self.name}")
            return
        
        self.logger.info(f"Final data prepared: {df_final.count()} rows")
        df_with_key = self.create_surrogate_key(df_final)
        self.logger.info("Surrogate key added")
        self.write_to_deltalake(df_with_key)
        self.logger.info("Pipeline completed successfully")

# COMMAND ----------

