# Databricks notebook source
# MAGIC %run ./gold_dim_prototype

# COMMAND ----------

gold_dim_dealer = Dim('dealer', columns=["dealer_id", "dealer_name"])
gold_dim_dealer.run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM car_catalog.gold.dim_dealer