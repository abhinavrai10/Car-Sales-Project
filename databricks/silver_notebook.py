# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = (spark.read.format("parquet")
      .option("inferSchema", "True")
      .load("abfss://bronze@storagecarproject.dfs.core.windows.net/rawdata")
      )
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Following Naming Standards

# COMMAND ----------

df = df.selectExpr(*[f"`{col}` as `{col.lower()}`" for col in df.columns])

# COMMAND ----------

df = df.withColumnRenamed("branchName", "branch_name").withColumnRenamed("dealername", "dealer_name")
df.display()

# COMMAND ----------

df = df.withColumn('model_category', split(col('model_id'), '-')[0])
df.display()

# COMMAND ----------

df.withColumn('units_sold', col('units_sold').cast(IntegerType())).display()

# COMMAND ----------

df = df.withColumn('rev_per_unit', col('revenue')/col('units_sold'))
df.display()

# COMMAND ----------

display(df.groupBy('year','branch_name').agg(sum('units_sold').alias('total_units')).sort('year', 'total_units', ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing to Silver Layer

# COMMAND ----------

df.write.format("parquet")\
        .mode("overwrite")\
        .option("path","abfss://silver@storagecarproject.dfs.core.windows.net/carsales")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`abfss://silver@storagecarproject.dfs.core.windows.net/carsales`

# COMMAND ----------

