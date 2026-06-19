# Databricks notebook source
# MAGIC %md Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
                .option('inferSchema', True)\
                .load('abfss://bronze@datalakecarsales.dfs.core.windows.net/rawdata')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md Data Transformation

# COMMAND ----------

# Transforming Model_ID to two parts using '-' seperator
from pyspark.sql.functions import *
df = df.withColumn('Model_category', split(col('Model_ID'), '-')[0])
display(df)

# COMMAND ----------

#RevSoldperUnit
df = df.withColumn('RevPerUnit', col('Revenue')/col('Units_Sold'))
display(df)

# COMMAND ----------

# MAGIC %md AD-HOC

# COMMAND ----------

#how many units sold per each branchname every year
from pyspark.sql.types import *
df.groupBy('Year', 'BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort("Year", "Total_Units", ascending = [1,0]).display()

# COMMAND ----------

#Data Writing
df.write.format('parquet')\
        .mode('OVERWRITE')\
        .option('path','abfss://silver@datalakecarsales.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@datalakecarsales.dfs.core.windows.net/carsales`

# COMMAND ----------

