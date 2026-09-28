import pyodbc
import logging

class SqlServer:

    def __init__(self, connection_string):
        self.connection_string = connection_string


    def schema_exists(self, schema_name) -> bool:
        query = f"SELECT * FROM sys.schemas WHERE name = '{schema_name}'"
        conn = pyodbc.connect(self.connection_string)
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchone() is not None


    def table_exists(self, schema_name, table_name) -> bool:
        query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}';"
        conn = pyodbc.connect(self.connection_string)
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchone() is not None


    def create_schema(self, schema_name):
        query = f"CREATE SCHEMA {schema_name}"
        self.execute_query(query)
    

    def get_column_info(self, schema_name, table_name) -> dict:
        query = f"SELECT [COLUMN_NAME], [CHARACTER_MAXIMUM_LENGTH] FROM INFORMATION_SCHEMA.COLUMNS WHERE [TABLE_SCHEMA] = '{schema_name}' AND [TABLE_NAME] = '{table_name}'; "
        conn = pyodbc.connect(self.connection_string)
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            res = {}
            while row:
                res[str(row[0])] = int(row[1])
                row = cursor.fetchone()
            return res

    
    def create_table(self, schema_name, table_name, attributes):
        query = f"CREATE TABLE [{schema_name}].[{table_name}] ("
        ap = [f"[{an}] NVARCHAR({attributes[an]}) NULL" for an in attributes]
        query += ",".join(ap)
        query += ");"
        self.execute_query(query)
        print("Done creating table")


    def execute_query(self, query):
        conn = pyodbc.connect(self.connection_string)
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)

    def execute_many(self, query, data):
        conn = pyodbc.connect(self.connection_string)
        with conn:
            cursor = conn.cursor()
            cursor.executemany(query, data)


