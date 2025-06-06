import sqlite3
import json
import datetime
from datetime import datetime, timezone, timedelta

def birth_format(date):
    
    try:
        birth = date["$date"]
        dt = datetime.fromisoformat(birth.replace("Z", "+00:00"))

        return dt.date().isoformat()

    except Exception:
        try:
            birth = date["$date"]
            millis = int(birth["$numberLong"])
            dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=millis)

            return dt.date().isoformat()

        except Exception:

            with open("errorcustomer.log", "a") as log_file:
                log_file.write(f"{username}\n")
            return None




# 1. Connect to SQLite db
conn = sqlite3.connect("DataWarehouseV4.db")
cursor = conn.cursor()

# 2. Load and process Customer data
with open("sample_analytics_dataset\sample_analytics.customers.json", "r") as f:
    customers = json.load(f)

for cust in customers:
    username = cust.get("username")
    name = cust.get("name")


    birthdate = cust.get("birthdate")

    bithdate_formate = birth_format(birthdate)


    try:
        cursor.execute("""
            INSERT INTO DimCustomer (username, name, birthdate)
            VALUES (?, ?, ?)
        """, (username, name, bithdate_formate))
        print("Query executed successfully.")
    except Exception as e:
        print("Query failed:", e)

# conn.commit()
# conn.close()
