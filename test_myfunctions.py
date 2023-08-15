
import pytest
import pyspark
import yaml
from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType


dbName      = "main.schema1"





# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# Create fake data for the unit tests to run against.
# In general, it is a best practice to not run unit tests
# against functions that work with data in production.





# Does the table exist?
def test_tableExists():

  path_name = "/Workspace/Repos/dishi.jain@versent.com.au/test/config_file/apa_mvp_config_file.yml"

  with open(path_name,"r") as file:
     file_contents = file.read()
  data_list = list(yaml.safe_load_all(file_contents))

  for sources in data_list:
    for table_details in sources['source_1']['tables']:
      if len(table_details['special_fields']) != 0 or len(table_details['column_mapping']) != 0:
        tableName = table_details['table_name']
        assert tableExists(tableName, dbName) is True
        


# Does the column exist?
def test_columnExists():
  path_name = "/Workspace/Repos/dishi.jain@versent.com.au/test/config_file/apa_mvp_config_file.yml"

  with open(path_name,"r") as file:
     file_contents = file.read()
  data_list = list(yaml.safe_load_all(file_contents))

  for sources in data_list:
    for table_details in sources['source_1']['tables']:
      if len(table_details['special_fields']) != 0 or len(table_details['column_mapping']) != 0:
        tableName = table_details['table_name']
        try:
          df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")
          if len(table_details['special_fields']) != 0:
            for addition_columns in table_details['special_fields']:
              columnName = addition_columns['field_id']
              assert columnExists(df, columnName) is True
          if len(table_details['column_mapping']) != 0:
            for renamed_columns in table_details['column_mapping']:
              columnName = renamed_columns['new_column_name']
              assert columnExists(df, columnName) is True
        except:
          pass




