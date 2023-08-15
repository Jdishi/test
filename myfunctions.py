%pip install pyyaml

import pyspark
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# Does the specified table exist in the specified database?

path_name = "/Workspace/Repos/dishi.jain@versent.com.au/test/config_file/apa_mvp_config_file.yml"

with open(path_name,"r") as file:
    file_contents = file.read()
data_list = list(yaml.safe_load_all(file_contents))

def tableExists(tableName, dbName):

    #print(data_list)

    for sources in data_list:
        for table_details in sources['source_1']['tables']:
            if len(table_details['special_fields']) != 0 and len(table_details['column_mapping']) != 0:
                tableName = table_details['table_name']
                try:
                    df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")
                    return True
                except:
                    return False

# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
  if columnName in dataFrame.columns:
    return True
  else:
    return False

# How many rows are there for the specified value in the specified column
# in the given DataFrame?
def numRowsInColumnForValue(dataFrame, columnName, columnValue):
  df = dataFrame.filter(col(columnName) == columnValue)

  return df.count()

