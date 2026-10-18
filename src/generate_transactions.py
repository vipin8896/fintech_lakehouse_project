import json
import os
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_RECORDS = 100_000
NUM_CUSTOMERS = 5_000

# Fixed customer pool: ek customer ki kai transactions hongi (realistic)
customers = [
    {
        "customer_id": f"CUST{i+1:05d}",
        "customer_name": fake.name(),
        "customer_email": fake.unique.email(),
        "iban": fake.iban(),
    }
    for i in range(NUM_CUSTOMERS)
]

# (description, weight)
descriptions = [
    ("Monthly Salary Payment", 8),
    ("SEPA Transfer", 20),
    ("Payment Reversal", 3),
    ("Card Purchase Supermarket", 30),
    ("Card Purchase Online Shop", 20),
    ("Utility Bill", 10),
    ("ATM Withdrawal", 9),
]
desc_names = [d[0] for d in descriptions]
desc_weights = [d[1] for d in descriptions]

start = datetime(2026, 10, 1)
end = datetime(2026, 10, 16, 23, 59, 59)
span_seconds = int((end - start).total_seconds())

def make_amount(desc):
    if desc == "Monthly Salary Payment":
        return round(random.uniform(1500, 5000), 2)       # positive = INFLOW
    if desc == "Payment Reversal":
        return round(random.uniform(5, 500), 2)           # positive = INFLOW
    if desc == "SEPA Transfer":
        sign = random.choice([1, -1])
        return round(sign * random.uniform(10, 2000), 2)
    return round(-random.uniform(2, 800), 2)              # negative = OUTFLOW

out_dir = "bronze_out/raw_transactions/year=2026/month=10"
os.makedirs(out_dir, exist_ok=True)
out_file = os.path.join(out_dir, "transactions_20261017.json")

with open(out_file, "w", encoding="utf-8") as f:
    for _ in range(NUM_RECORDS):
        c = random.choice(customers)
        desc = random.choices(desc_names, weights=desc_weights)[0]
        ts = start + timedelta(seconds=random.randint(0, span_seconds))
        record = {
            "transaction_id": str(uuid.uuid4()),
            "source_account_iban": c["iban"],
            "target_account_iban": fake.iban(),
            "amount": make_amount(desc),
            "currency": random.choices(["EUR", "USD", "QAR"], weights=[60, 25, 15])[0],
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "description": desc,
            "customer_id": c["customer_id"],
            "customer_email": c["customer_email"],
            "customer_name": c["customer_name"],
            "status": random.choices(["COMPLETED", "PENDING", "FAILED"], weights=[90, 6, 4])[0],
        }
        f.write(json.dumps(record) + "\n")

print(f"Done: {NUM_RECORDS} records written to {out_file}")