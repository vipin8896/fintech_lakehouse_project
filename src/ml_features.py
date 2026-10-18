# Databricks notebook source
STORAGE_ACCOUNT = "fintechlakedevtj3oe5"
STORAGE_KEY = "STORAGE_KEY = dbutils.secrets.get(scope="kv-scope", key="storage-key"

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY.strip())

silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions_delta"
feature_path = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/ml_features/customer_risk_v1/"

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.read.format("delta").load(silver_path)

ref_date = silver.agg(F.max("transaction_date")).collect()[0][0]

recent = (silver
    .filter(F.col("status") != "FAILED")
    .filter(F.col("transaction_date") >= F.date_sub(F.lit(ref_date).cast("date"), 30)))

feature_df = (recent.groupBy("customer_id_hashed").agg(
    F.avg(F.when(F.col("direction") == "INFLOW", F.col("amount"))).alias("avg_inflow_amount"),
    F.countDistinct(F.when(F.col("direction") == "INFLOW", F.to_date("transaction_date"))).alias("inflow_frequency_days"),
    (F.sum(F.when(F.col("category") == "SALARY", 1).otherwise(0)) >= 1).cast("int").alias("salary_indicator_flag"),
    F.stddev(F.col("amount").cast("double")).alias("stddev_transaction_amount"),
    F.count("*").alias("txn_count_30d"),
))

feature_df.show(5)
print(feature_df.count())

# COMMAND ----------

feature_df.write.mode("overwrite").parquet(feature_path)
print(spark.read.parquet(feature_path).count())

# COMMAND ----------

