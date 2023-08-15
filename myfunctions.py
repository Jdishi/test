

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# Does the specified table exist in the specified database?



def tableExists(tableName, dbName):
    try:
        df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")
        print(f"Table {tableName} exist")
        return True
    except:
        print(f"Table {tableName} DOES NOT exist")
        return False

# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
  if columnName in dataFrame.columns:
    print(f"Column {columnName} exist")
    return True 
  else:
    print(f"Column {columnName} DOES NOT exist")
    return False

# How many rows are there for the specified value in the specified column
# in the given DataFrame?

