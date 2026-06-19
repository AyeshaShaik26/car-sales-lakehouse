# Databricks notebook source
# MAGIC %md create flag parameter

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

    dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
#%mdprint(type(incremental_flag))

# COMMAND ----------

# MAGIC %md
# MAGIC Create Dimensional Model - Dim_Model

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@datalakecarsales.dfs.core.windows.net/carsales` limit 10

# COMMAND ----------

df_source = spark.sql('''select distinct(Model_ID) as Model_ID, Model_category
from parquet.`abfss://silver@datalakecarsales.dfs.core.windows.net/carsales`''')
display(df_source)


# COMMAND ----------

# MAGIC %md dim_model initial and incremental check

# COMMAND ----------

if spark.catalog.tableExists('car_catalog.gold.dim_model'):
    
    df_sink = spark.sql ('''SELECT dim_model_key, Model_ID, Model_category
    from car_catalog.gold.dim_model
    ''')
    
else:

    df_sink = spark.sql ('''SELECT 1 AS dim_model_key, Model_ID, Model_category
    from parquet.`abfss://silver@datalakecarsales.dfs.core.windows.net/carsales`
    where 1=0''')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

# MAGIC %md filtering old and new records and pulling existing data

# COMMAND ----------

df_filter = df_source.join(df_sink, df_source.Model_ID == df_sink.Model_ID, 'left').select(df_source.Model_ID, df_source.Model_category, df_sink.dim_model_key)


# COMMAND ----------

# MAGIC %md filtering old records 

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

display(df_filter_old)

# COMMAND ----------

# MAGIC %md filtering new records

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_source.Model_ID, df_source.Model_category)

# COMMAND ----------

display(df_filter_new)

# COMMAND ----------

# MAGIC %md fetch max surrogate key

# COMMAND ----------

if (incremental_flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql('''select max(dim_model_key) from cars_catalog.gold.dim_model''')
    max_value = max_value_df.collect()[0][0]


# COMMAND ----------

# MAGIC %md create or update surrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key', max_value+monotonically_increasing_id())


# COMMAND ----------

display(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC create final dataframe with old and new df's

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md SCD type 1 - upsert (update + insert/merge)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#incremental run
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@datalakecarsales.dfs.core.windows.net/dim_model')    
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_key = src.dim_model_key')\
                            .whenMatchedUpdateAll()\
                            .whenNotMatchedInsertAll()\
                            .execute()
#Initial Run
else:
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path', 'abfss://gold@datalakecarsales.dfs.core.windows.net/dim_model')\
            .saveAsTable('cars_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model