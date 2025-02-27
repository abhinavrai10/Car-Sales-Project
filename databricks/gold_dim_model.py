# Databricks notebook source
# MAGIC %run ./gold_dim_prototype

# COMMAND ----------

gold_dim_model = Dim('model', columns=["model_id", "model_category"])
gold_dim_model.run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM car_catalog.gold.dim_model