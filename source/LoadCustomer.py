import sqlite3
import json
from datetime import datetime, timezone, timedelta

def SQL_Date_Format(date):
    
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

            with open("ErrorCustomerDate.log", "a") as log_file:
                log_file.write(f"{username}-> {date['$date']}\n")
            return None




# 1. Connect to SQLite db
conn = sqlite3.connect("DataWarehouseV6.db")
cursor = conn.cursor()

# 2. Load and process Customer data
with open("sample_analytics_dataset\sample_analytics.customers.json", "r") as f:
    customers = json.load(f)

for cust in customers:
    username = cust.get("username")
    name = cust.get("name")
    cod_customer = username + name


    birthdate = cust.get("birthdate")

    bithdate_formate = SQL_Date_Format(birthdate)


    try:
        cursor.execute("""
            INSERT OR IGNORE INTO DimCustomer (cod_customer,username, name, birthdate)
            VALUES (?, ?, ?, ?)
        """, (cod_customer,username, name, bithdate_formate))
        customer_id = cursor.lastrowid
        #print("Query executed successfully.")
    except Exception as e:
        print("Query failed:", e)
        with open("ErrorCustomerInsert.log", "a") as log_file:
                log_file.write(f"{username}\n")

# In this point, the DimCustomer es fill with all customers
 

    tier_and_details = cust.get("tier_and_details", {})
    print( tier_and_details.items())

    for tier_id, tier_data in tier_and_details.items():
        tier_name = tier_data.get("tier")
        active = tier_data.get("active", True)

        # Insertar tier in DimTier

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO DimTier (tier_id, tier, active)
                VALUES (?, ?, ?)
            """, (tier_id, tier_name, active))
            #print("Query executed successfully.")
        except Exception as e:
            print("Query failed:", e)
        
        cursor.execute("""
            INSERT OR IGNORE INTO BridgeCustomerTier (customer_id, tier_id)
            VALUES (?, ?)
        """, (customer_id, tier_id))


        

        for benefit in tier_data.get("benefits"):
            cursor.execute("""
                    INSERT OR IGNORE INTO DimBenefit (benefit_name)
                    VALUES (?)
                """, (benefit,))
            

            cursor.execute("""
                SELECT benefit_id FROM DimBenefit WHERE benefit_name = ?
            """, (benefit,))

            benefit_id = cursor.fetchone()[0]


            cursor.execute("""
                INSERT OR IGNORE INTO BridgeTierBenefit (tier_id, benefit_id)
                VALUES (?, ?)
            """, (tier_id, benefit_id))
        

with open("sample_analytics_dataset\sample_analytics.accounts.json", "r") as f:
    accounts = json.load(f)

for acc in accounts:
    account_id = acc.get("account_id")
    account_limit = acc.get("limit")

    customer_id = None
    for cust in customers:
        if account_id in cust.get("accounts", []):
            username = cust.get("username")
            name = cust.get("name")
            cod_customer = username + name
            # Recuperar el customer_id desde DimCustomer
            cursor.execute("""
                SELECT customer_id FROM DimCustomer WHERE cod_customer = ?
            """, (cod_customer,))
            result = cursor.fetchone()
            customer_id = result[0] if result else None
            break

    cursor.execute("""
        INSERT OR IGNORE INTO DimAccount (account_id, account_limit, customer_id)
        VALUES (?, ?, ?)
    """, (account_id, account_limit, customer_id))

    # Insertar productos asociados a la cuenta
    products = acc.get("products", [])
    for product_name in products:
        cursor.execute("""
            INSERT OR IGNORE INTO DimProduct (product_name)
            VALUES (?)
        """, (product_name,))
        cursor.execute("""
            SELECT product_id FROM DimProduct WHERE product_name = ?
        """, (product_name,))
        product_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT OR IGNORE INTO BridgeAccountProduct (account_id, product_id)
            VALUES (?, ?)
        """, (account_id, product_id))

# 4️⃣ Cargar y procesar Transactions
with open("sample_analytics_dataset\sample_analytics.transactions.json", "r") as f:
    transactions = json.load(f)

for acc_trans in transactions:
    account_id = acc_trans.get("account_id")
    transactions_list = acc_trans.get("transactions", [])

    # Recuperar el customer_id desde DimAccount
    cursor.execute("""
        SELECT customer_id FROM DimAccount WHERE account_id = ?
    """, (account_id,))
    result = cursor.fetchone()
    customer_id = result[0] if result else None

    for t in transactions_list:
        date = t.get("date")
        date = SQL_Date_Format(date)
        amount = t.get("amount")
        total = t.get("total")
        transaction_code = t.get("transaction_code")
        symbol = t.get("symbol")

        # Insertar transacción (transaction_id autoincrement)
        cursor.execute("""
            INSERT INTO FactTransaction (account_id, customer_id, date, amount, total, transaction_code, symbol)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (account_id, customer_id, date, amount, total, transaction_code, symbol))

    

conn.commit()
conn.close()
