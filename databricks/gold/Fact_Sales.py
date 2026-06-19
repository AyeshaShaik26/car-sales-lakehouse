# Databricks notebook source
# MAGIC %md Create Fact sales

# COMMAND ----------

# MAGIC %md reading silver data to check fact columns

# COMMAND ----------

df_silver = spark.sql(''' 
                      select * from parquet.`abfss://silver@datalakecarsales.dfs.core.windows.net/carsales`
                       ''')

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md reading all dims since we need joining keys

# COMMAND ----------

    df_model = spark.sql('select * from cars_catalog.gold.dim_model')
    df_branch = spark.sql('select * from cars_catalog.gold.dim_branch')
    df_dealer = spark.sql('select * from cars_catalog.gold.dim_dealer')
    df_date = spark.sql('select * from cars_catalog.gold.dim_date')

# COMMAND ----------

# MAGIC %md bringing required numeric columns and applying join dims

# COMMAND ----------

df_fact = df_silver.join(df_model, df_silver.Model_ID == df_model.Model_ID, 'left')\
                    .join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID, 'left')\
                    .join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID, 'left')\
                    .join(df_date, df_silver.Date_ID == df_date.Date_ID, 'left')\
                    .select(df_silver.Revenue, df_silver.Units_Sold, df_silver.RevPerUnit, df_model.dim_model_key, df_branch.dim_branch_key, df_dealer.dim_dealer_key, df_date.dim_date_key)

# COMMAND ----------

# MAGIC %md Writing fact table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.factsales'):
    delta_tbl= DeltaTable.forName(spark, 'cars_catalog.gold.factsales')
    delta_tbl.alias('trg').merge(df_fact.alias('src'), 'trg.dim_model_key = src.dim_model_key and trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_date_key = src.dim_date_key')\
                    .whenMatchedUpdateAll()\
                    .whenNotMatchedInsertAll()\
                    .execute()
else:

    df_fact.write.format('delta')\
        .mode('overwrite')\
        .option('path', 'abfss://gold@datalakecarsales.dfs.core.windows.net/factsales')\
        .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales