import json
import pandas as pd

# Cargar datos
with open("sample_analytics_dataset\sample_analytics.customers.json", "r") as f:
    data = json.load(f)

# Extraer campos relevantes
usernames = []
names = []
all_accounts = []

for record in data:
    usernames.append(record.get("username", ""))
    names.append(record.get("name", ""))
    accounts = record.get("accounts", [])
    all_accounts.extend(accounts)

# Crear DataFrames
df_usernames = pd.Series(usernames, name="username")
df_names = pd.Series(names, name="name")
df_accounts = pd.Series(all_accounts, name="account")


# Detectar duplicados
duplicate_usernames = df_usernames.value_counts()
duplicate_usernames = duplicate_usernames[duplicate_usernames > 1]

duplicate_names = df_names.value_counts()
duplicate_names = duplicate_names[duplicate_names > 1]

duplicate_accounts = df_accounts.value_counts()
duplicate_accounts = duplicate_accounts[duplicate_accounts > 1]

# Guardar logs
with open("source\Verify\duplicates_usernames.log", "w") as f:
    if not duplicate_usernames.empty:
        f.write(duplicate_usernames.to_string())
    else:
        f.write("No duplicate usernames found.\n")

with open("source\Verify\duplicates_names.log", "w") as f:
    if not duplicate_names.empty:
        f.write(duplicate_names.to_string())
    else:
        f.write("No duplicate names found.\n")

with open("source\Verify\duplicates_accounts.log", "w") as f:
    if not duplicate_accounts.empty:
        f.write(duplicate_accounts.to_string())
    else:
        f.write("No duplicate accounts found.\n")