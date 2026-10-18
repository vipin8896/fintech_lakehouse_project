# Databricks notebook source
STORAGE_ACCOUNT = "fintechlakedevtj3oe5"
STORAGE_KEY = dbutils.secrets.get(scope="kv-scope", key="storage-key")

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY.strip())

bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_transactions/*/*/*.json"
silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/transactions_delta"
gold_fact   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/fact_payments"

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Load

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.read.json(bronze_path)
silver = spark.read.format("delta").load(silver_path)
gold   = spark.read.parquet(gold_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC #Quality Checks

# COMMAND ----------

results = []

def check(name, passed, detail=""):
    results.append((name, bool(passed), detail))

total = silver.count()

check("transaction_id not null", silver.filter(F.col("transaction_id").isNull()).count() == 0)
check("transaction_id unique", silver.select("transaction_id").distinct().count() == total)
check("amount not null", silver.filter(F.col("amount").isNull()).count() == 0)
check("amount not zero", silver.filter(F.col("amount") == 0).count() == 0)

bad_ccy = silver.filter(~F.col("currency").isin("EUR", "USD", "QAR")).count()
check("currency in EUR/USD/QAR", bad_ccy == 0, f"bad rows: {bad_ccy}")

bad_status = silver.filter(~F.col("status").isin("COMPLETED", "PENDING", "FAILED")).count()
check("status valid", bad_status == 0, f"bad rows: {bad_status}")

check("PII columns removed", not any(c in silver.columns for c in
      ["customer_email", "customer_name", "source_account_iban", "target_account_iban"]))

check("direction matches amount sign",
      silver.filter(((F.col("amount") > 0) & (F.col("direction") != "INFLOW")) |
                    ((F.col("amount") < 0) & (F.col("direction") != "OUTFLOW"))).count() == 0)

for r in results:
    print("PASS" if r[1] else "FAIL", "|", r[0], r[2])

# COMMAND ----------

# MAGIC %md
# MAGIC #Reconciliation (Bronze vs Silver vs Gold)

# COMMAND ----------

def stats(df):
    row = df.agg(F.count("*").alias("cnt"),
                 F.sum(F.col("amount").cast("decimal(18,2)")).alias("total")).collect()[0]
    return row["cnt"], row["total"]

b_cnt, b_sum = stats(bronze)
s_cnt, s_sum = stats(silver)
g_cnt, g_sum = stats(gold)

print(f"Bronze: count={b_cnt}, sum={b_sum}")
print(f"Silver: count={s_cnt}, sum={s_sum}")
print(f"Gold  : count={g_cnt}, sum={g_sum}")

check("Bronze == Silver (count)", b_cnt == s_cnt, f"{b_cnt} vs {s_cnt}")
check("Bronze == Silver (sum)",   b_sum == s_sum, f"{b_sum} vs {s_sum}")
check("Silver == Gold (count)",   s_cnt == g_cnt, f"{s_cnt} vs {g_cnt}")
check("Silver == Gold (sum)",     s_sum == g_sum, f"{s_sum} vs {g_sum}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Quality gate

# COMMAND ----------

failed = [r for r in results if not r[1]]

for r in results:
    print("PASS" if r[1] else "FAIL", "|", r[0], r[2])

if failed:
    raise Exception(f"Data Quality FAILED: {len(failed)} check(s) failed. Pipeline stopped.")
print("All checks passed.")

# COMMAND ----------

