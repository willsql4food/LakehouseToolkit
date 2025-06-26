# Databricks notebook source
def fact(x):
    if(x > 1):
        return fact(x - 1) * x
    else:
        return x
    
for i in range(1, 10):
    print(f"{i} factorial is {fact(i)}")


# COMMAND ----------


