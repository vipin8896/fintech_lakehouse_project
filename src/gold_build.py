# Databricks notebook source
STORAGE_ACCOUNT = "fintechlakedevtj3oe5"
STORAGE_KEY = "STORAGE_KEY = dbutils.secrets.get(scope="kv-scope", key="storage-key")"

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY.strip())

silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions_delta"
gold_base   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.read.format("delta").load(silver_path)
print(silver.count())

# COMMAND ----------

fact_payments = (
    silver
    .withColumn("date_key", F.date_format("transaction_date", "yyyyMMdd").cast("int"))
    .select("transaction_id", "customer_id_hashed", "date_key",
            "amount", "currency", "direction", "category", "status")
)
fact_payments.write.mode("overwrite").parquet(f"{gold_base}/fact_payments")
print(fact_payments.count())

# COMMAND ----------

dim_customer = (
    silver.groupBy("customer_id_hashed")
    .agg(
        F.min(F.to_date("transaction_date")).alias("first_transaction_date"),
        F.count("*").alias("txn_count"),
    )
    .withColumn(
        "customer_tier",
        F.when(F.col("txn_count") >= 25, "GOLD")
         .when(F.col("txn_count") >= 15, "SILVER")
         .otherwise("BRONZE"),
    )
    .drop("txn_count")
)
dim_customer.write.mode("overwrite").parquet(f"{gold_base}/dim_customer")
print(dim_customer.count())

# COMMAND ----------

dim_date = (
    silver.select(F.to_date("transaction_date").alias("full_date")).distinct()
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
)
dim_date.write.mode("overwrite").parquet(f"{gold_base}/dim_date")
print(dim_date.count())

# COMMAND ----------

print(fact_payments.agg(F.count("*"), F.sum("amount")).collect())
print(silver.agg(F.count("*"), F.sum("amount")).collect())

# COMMAND ----------

dim_customer_out = dim_customer.select("customer_id_hashed", "customer_tier", "first_transaction_date")
dim_customer_out.write.mode("overwrite").parquet(f"{gold_base}/dim_customer")

dim_date_out = dim_date.select("date_key", "full_date", "year", "month", "day_of_week", "is_weekend")
dim_date_out.write.mode("overwrite").parquet(f"{gold_base}/dim_date")

# COMMAND ----------

