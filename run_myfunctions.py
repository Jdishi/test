from myfunctions import *
import yaml

dbName      = "main.schema1"
columnName  = "id"
columnValue = "1"

path_name = "/Workspace/Repos/dishi.jain@versent.com.au/test/config_file/apa_mvp_config_file.yml"

with open(path_name,"r") as file:
    file_contents = file.read()
data_list = list(yaml.safe_load_all(file_contents))

for sources in data_list:
    for table_details in sources['source_1']['tables']:
        if len(table_details['special_fields']) != 0 and len(table_details['column_mapping']) != 0:
            tableName = table_details['table_name']

            if tableExists(tableName, dbName):

                df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")

                # And the specified column exists in that table...
                if columnExists(df, columnName):
                    # Then report the number of rows for the specified value in that column.
                    numRows = numRowsInColumnForValue(df, columnName, columnValue)

                    print(f"There are {numRows} rows in '{tableName}' where '{columnName}' equals '{columnValue}'.")
                else:
                    print(f"Column '{columnName}' does not exist in table '{tableName}' in schema (database) '{dbName}'.")
            else:
                print(f"Table '{tableName}' does not exist in schema (database) '{dbName}'.") 