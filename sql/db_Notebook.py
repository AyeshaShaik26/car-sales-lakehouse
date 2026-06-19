# Databricks notebook source
# MAGIC %md Create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog cars_catalog

# COMMAND ----------

# MAGIC %md Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold

# COMMAND ----------

