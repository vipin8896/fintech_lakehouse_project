# Databricks notebook source
# MAGIC %md
# MAGIC #Storage connection

# COMMAND ----------

STORAGE_ACCOUNT = "fintechlakedevtj3oe5"
STORAGE_KEY = dbutils.secrets.get(scope="kv-scope", key="storage-key")

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_transactions/*/*/*.json"
silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions_delta"

# COMMAND ----------

spark.read.format("delta").load(silver_path).count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze Read
# MAGIC

# COMMAND ----------

raw_df = spark.read.json(bronze_path)
print(raw_df.count())
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Clean, type cast, PII hash

# COMMAND ----------

from pyspark.sql import functions as F

SALT = "SaltKey-learning-2026"  # sirf practice; baad me Key Vault se lenge

clean_df = (
    raw_df
    .dropDuplicates(["transaction_id"])
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("transaction_date", F.to_timestamp("timestamp"))
    .withColumn("direction", F.when(F.col("amount") > 0, "INFLOW").otherwise("OUTFLOW"))
    .withColumn("customer_id_hashed", F.sha2(F.concat(F.col("customer_email"), F.lit(SALT)), 256))
    .withColumn(
        "category",
        F.when(F.lower(F.col("description")).like("%salary%"), "SALARY")
         .when(F.lower(F.col("description")).like("%sepa%"), "TRANSFER")
         .when(F.lower(F.col("description")).like("%reversal%"), "REVERSAL")
         .otherwise("GENERAL"),
    )
    .withColumn("ingested_at", F.current_timestamp())
    .drop("customer_name", "customer_email", "source_account_iban", "target_account_iban", "customer_id")
)

clean_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Delta MERGE (upsert)

# COMMAND ----------

from delta.tables import DeltaTable

if not DeltaTable.isDeltaTable(spark, silver_path):
    clean_df.write.format("delta").mode("overwrite").save(silver_path)
else:
    (DeltaTable.forPath(spark, silver_path).alias("target")
        .merge(clean_df.alias("source"), "target.transaction_id = source.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC #Optimize and verify

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (transaction_date, category)")
print(spark.read.format("delta").load(silver_path).count())

# COMMAND ----------

print(len(dbutils.secrets.get(scope="kv-scope", key="storage-key")))

# COMMAND ----------

