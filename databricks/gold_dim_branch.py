# Databricks notebook source
# MAGIC %run ./gold_dim_prototype

# COMMAND ----------

gold_dim_br = Dim('Branch', columns=["branch_id", "branch_name"])
gold_dim_br.run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM car_catalog.gold.dim_branch