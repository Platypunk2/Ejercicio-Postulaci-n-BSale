import polars as pl
import json
import sqlite3


df = pl.read_json("sample_analytics_dataset\sample_analytics_dataset\sample_analytics.accounts.json")

print(df)