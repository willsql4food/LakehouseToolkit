# Databricks notebook source
scopes = dbutils.secrets.listScopes()
for sc in scopes:
    print(f"========================================\n{sc.name}\n-----------------------------------")
    secrets = dbutils.secrets.list(scope=sc.name)
    for s in secrets:
        print(s.key)


# COMMAND ----------


