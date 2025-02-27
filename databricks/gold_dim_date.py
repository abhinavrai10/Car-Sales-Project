# Databricks notebook source
# MAGIC %run ./gold_dim_prototype

# COMMAND ----------

gold_dim_date = Dim('date', columns=["date_id"])
gold_dim_date.run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM car_catalog.gold.dim_date