# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Sales

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------

def read_silver_data(path):
    """Read silver layer data from specified path"""
    return spark.sql(f"SELECT * FROM PARQUET.`{path}`")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading all the DIMENSIONS**

# COMMAND ----------

def read_dimension_table(table_name):
    """Read dimension table from gold layer"""
    return spark.sql(f"SELECT * FROM car_catalog.gold.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC **Bring Keys to the Fact Table**

# COMMAND ----------

def create_fact_table(
    silver_df,
    branch_df,
    dealer_df,
    date_df,
    model_df):
    """Create fact table by joining silver data with dimension tables"""
    return (silver_df
            .join(branch_df, silver_df.branch_id == branch_df.branch_id, 'left')
            .join(dealer_df, silver_df.dealer_id == dealer_df.dealer_id, 'left')
            .join(date_df, silver_df.date_id == date_df.date_id, 'left')
            .join(model_df, silver_df.model_id == model_df.model_id, 'left')
            .select(
                silver_df.revenue,
                silver_df.units_sold,
                silver_df.rev_per_unit,
                branch_df.dim_branch_key,
                dealer_df.dim_dealer_key,
                date_df.dim_date_key,
                model_df.dim_model_key
            ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Fact Table

# COMMAND ----------

def write_fact_table(
    df,
    table_name: str = 'car_catalog.gold.fact_sales',
    storage_path: str = 'abfss://gold@storagecarproject.dfs.core.windows.net/fact_sales'
) -> None:
    """Write fact table to gold layer with merge logic"""
    if spark.catalog.tableExists(table_name):
        delta_table = DeltaTable.forName(spark, table_name)
        (delta_table.alias('trg')
         .merge(
             df.alias('src'),
             'trg.dim_branch_key = src.dim_branch_key AND '
             'trg.dim_dealer_key = src.dim_dealer_key AND '
             'trg.dim_date_key = src.dim_date_key AND '
             'trg.dim_model_key = src.dim_model_key'
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        (df.write
         .format('delta')
         .mode('overwrite')
         .option('path', storage_path)
         .saveAsTable(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@storagecarproject.dfs.core.windows.net/carsales`

# COMMAND ----------

def main():
    """Main function to orchestrate the fact table creation"""
    
    # Read silver data
    silver_path = 'abfss://silver@storagecarproject.dfs.core.windows.net/carsales'
    df_silver = read_silver_data(silver_path)
    
    # Read dimension tables
    df_dealer = read_dimension_table('dim_dealer')
    df_branch = read_dimension_table('dim_branch')
    df_date = read_dimension_table('dim_date')
    df_model = read_dimension_table('dim_model')
    
    # Create fact table
    df_fact = create_fact_table(df_silver, df_branch, df_dealer, df_date, df_model)
    
    # Write fact table
    write_fact_table(df_fact)
    
    # Optional: Display results
    df_fact.display()

if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM car_catalog.gold.fact_sales