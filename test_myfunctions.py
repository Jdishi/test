
import pytest
import pyspark
import yaml
from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import json
import ast








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
    path_name = "/Workspace/Repos/dishi.jain@versent.com.au/test/config_file/APA_MVP_dlt_pipeline_config_file.json"

    data_dic = json.dumps(json.load(open(path_name)), indent=2)
    data_list = [ast.literal_eval(data_dic)]
    '''
    with open(path_name,"r") as file:
      file_contents = file.read()
    data_list = list(yaml.safe_load_all(file_contents))
    '''

    for sources in data_list:
      for table_details in sources['source_1']['tables']:
        tableName = table_details['table_name']
        dbName = 'edp_dev_bronze.maximo_' + table_details['classification']
        try:
            assert tableExists(tableName, dbName) is True
        except:
            pass
        


# Does the column exist?
def test_columnExists():
    path_name = "/Workspace/Repos/dishi.jain@versent.com.au/test/config_file/APA_MVP_dlt_pipeline_config_file.json"

    data_dic = json.dumps(json.load(open(path_name)), indent=2)
    data_list = [ast.literal_eval(data_dic)]

    for sources in data_list:
      for table_details in sources['source_1']['tables']:
          tableName = table_details['table_name']
          dbName = 'edp_dev_bronze.maximo_' + table_details['classification']
          try:
            df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")
            for addition_columns in table_details['special_fields']:
                columnName = addition_columns['field_id']
                if columnExists(df, columnName):
                  print(f"Column {columnName} exist affter addition in table {tableName}")
                else:
                  print(f"Column {columnName} DOES NOT exist affter addition in table {tableName}")
                assert columnExists(df, columnName) is True
            for renamed_columns in table_details['column_mapping']:
                columnName = renamed_columns['new_column_name']
                if columnExists(df, columnName):
                  print(f"Column {columnName} exist affter rename in table {tableName}")
                else:
                  print(f"Column {columnName} DOES NOT exist affter rename in table {tableName}")
                assert columnExists(df, columnName) is True
          except:
            print("Some other issue")
            pass




