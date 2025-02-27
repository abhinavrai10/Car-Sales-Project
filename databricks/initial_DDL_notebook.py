# Databricks notebook source
# MAGIC %md
# MAGIC # Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS car_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS car_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS car_catalog.gold;