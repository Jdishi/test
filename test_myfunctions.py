import pytest
import pyspark
from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

tableName   = "courses_details"
dbName      = "main.schema1"
columnName  = "id"
columnValue = "1"

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# Create fake data for the unit tests to run against.
# In general, it is a best practice to not run unit tests
# against functions that work with data in production.


df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")

# Does the table exist?
def test_tableExists():
  assert tableExists(tableName, dbName) is True

# Does the column exist?
def test_columnExists():
  assert columnExists(df, columnName) is True

# Is there at least one row for the value in the specified column?
def test_numRowsInColumnForValue():
  assert numRowsInColumnForValue(df, columnName, columnValue) > 0