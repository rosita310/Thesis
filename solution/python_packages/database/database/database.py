"""
Shared database access module for the IM9906 thesis project.

Provides two database backends (SqlServer and Postgress) and a base Generic class
with common query execution helpers. Both backends connect via pyodbc.

Usage:
    from python_packages.database import Postgress, Saver

    db = Postgress(server='localhost', database='mydb', user='user', password='pass')
    saver = Saver(db)
    saver.save('my_schema', 'my_table', [{'col1': 'val1', 'col2': 'val2'}])

Classes:
    Generic    -- Base class with shared query methods (execute_query, execute_many, etc.)
    SqlServer  -- SQL Server backend via pyodbc
    Postgress  -- PostgreSQL backend via pyodbc
"""

import pyodbc
import logging

class Generic:
    """Base class providing shared query execution methods for database backends."""

    def execute_bool(self, query) -> bool:
        """Execute a query and return True if at least one row is returned."""
        logging.debug(f"Executing query: {query}")
        conn = self.get_connection()
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchone() is not None


    def execute_query(self, query):
        """Execute a query that returns no result (DDL or DML without output)."""
        logging.debug(query)
        conn = self.get_connection()
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)


    def execute_query_result(self, query) -> list:
        """Execute a query and return results as a list of dicts keyed by column name."""
        logging.debug(f"Exeuting query: {query}")
        conn = self.get_connection()
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            result = []
            columns = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                result.append(dict(zip(columns, row)))
            return result



    def execute_many(self, query, data):
        """Execute a parameterised query for each row in data (bulk insert)."""
        conn = self.get_connection()
        with conn:
            cursor = conn.cursor()
            cursor.executemany(query, data)


    def create_schema(self, schema_name):
        """Create a new database schema."""
        query = f"CREATE SCHEMA {schema_name}"
        self.execute_query(query)


    

    

class SqlServer(Generic):
    """SQL Server database backend using pyodbc.

    Used by scripts that connect to a local SQL Server instance.

    Args:
        connection_string: Full pyodbc connection string for SQL Server.
    """

    def __init__(self, connection_string):
        self.connection_string = connection_string


    def get_connection(self):
        """Return a new pyodbc connection to SQL Server."""
        return pyodbc.connect(self.connection_string)


    def schema_exists(self, schema_name) -> bool:
        """Return True if the given schema exists in the SQL Server database."""
        query = f"SELECT * FROM sys.schemas WHERE name = '{schema_name}'"
        return self.execute_bool(query)


    def table_exists(self, schema_name, table_name) -> bool:
        """Return True if the given table exists in the SQL Server database."""
        query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}';"
        return self.execute_bool(query)
    

    def get_column_info(self, schema_name, table_name) -> dict:
        """Return a dict mapping column names to their maximum character length."""
        query = f"SELECT [COLUMN_NAME], [CHARACTER_MAXIMUM_LENGTH] FROM INFORMATION_SCHEMA.COLUMNS WHERE [TABLE_SCHEMA] = '{schema_name}' AND [TABLE_NAME] = '{table_name}'; "
        conn = self.get_connection()
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
        """Create a table with NVARCHAR columns sized according to the attributes dict."""
        query = f"CREATE TABLE [{schema_name}].[{table_name}] ("
        ap = [f"[{an}] NVARCHAR({attributes[an]}) NULL" for an in attributes]
        query += ",".join(ap)
        query += ");"
        self.execute_query(query)
        logging.debug("Done creating table")


    def change_size(self, schema_name, table_name, column_name, new_size):
        """Alter a column's NVARCHAR max length."""
        query = f"ALTER TABLE [{schema_name}].[{table_name}] ALTER COLUMN [{column_name}] NVARCHAR({new_size}) NULL "
        self.execute_query(query)


    def add_columns(self, schema_name, table_name, columns):
        """Add one or more NVARCHAR columns to an existing table."""
        query = f"ALTER TABLE [{schema_name}].[{table_name}] ADD "
        column_parts = [f"[{c}] NVARCHAR({columns[c]}) NULL " for c in columns]
        query += ", ".join(column_parts) + ";"
        self.execute_query(query)


    def insert_into(self, schema_name, table_name, data):
        """Insert a list of dicts into the given table using parameterised queries."""
        db_data = [tuple(dic.values()) for dic in data]
        columns = ", ".join(["[" + x + "]" for x in data[0]])
        qm = ", ".join(["?"] * len(data[0]))
        query = f"INSERT INTO [{schema_name}].[{table_name}] ({columns}) VALUES ({qm})"
        self.execute_many(query, db_data)


class Postgress(Generic):
    """PostgreSQL database backend using pyodbc (PostgreSQL ODBC Driver).

    Current backend for Python scrapers.

    Args:
        server:   Hostname or IP of the PostgreSQL server.
        database: Name of the target database.
        user:     Database user name.
        password: Database password.
    """


    def __init__(self, server, database, user, password):
        self.connection_string = (
            "DRIVER={PostgreSQL ODBC Driver(UNICODE)};"
            f"Server={server};"
            "Port=5432;"
            f"Database={database};"
            f"uid={user};"
            f"pwd={password};"
            # sslmode=require is disabled for local Docker usage
            "sslmode=disable;"
        )


    def get_connection(self):
        """Return a new pyodbc connection to PostgreSQL."""
        return pyodbc.connect(self.connection_string)


    def schema_exists(self, schema_name) -> bool:
        """Return True if the given schema exists in the PostgreSQL database."""
        logging.info("Posgres Schema exists")
        query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';"
        return self.execute_bool(query)


    def table_exists(self, schema_name, table_name) -> bool:
        """Return True if the given table exists in the PostgreSQL database."""
        query = f"""
        SELECT 1
        FROM   information_schema.tables 
        WHERE  table_schema = '{schema_name}'
        AND    table_name = '{table_name}'
        """
        return self.execute_bool(query)
    

    def get_column_info(self, schema_name, table_name) -> dict:
        """Return a dict mapping column names to their character octet length.

        For TEXT columns in PostgreSQL, CHARACTER_OCTET_LENGTH reflects the stored
        byte size rather than a declared max length (TEXT has no max length).
        """
        query = f"""
        SELECT
            COLUMN_NAME, CHARACTER_OCTET_LENGTH
        FROM
            information_schema.columns
        WHERE
            table_schema = '{schema_name}'
            AND table_name = '{table_name}';
        """
        conn = self.get_connection()
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
        """Create a table with TEXT columns (PostgreSQL has no NVARCHAR max-length limit)."""
        query = f"CREATE TABLE \"{schema_name}\".\"{table_name}\" ("
        ap = [f"\"{an}\" TEXT NULL" for an in attributes]
        query += ",".join(ap)
        query += ");"
        self.execute_query(query)
        print("Done creating table")


    def change_size(self, schema_name, table_name, column_name, new_size):
        """No-op for PostgreSQL: TEXT columns have no maximum length to alter."""
        # TEXT in PostgreSQL is unlimited — resizing is not needed or possible.
        pass


    def add_columns(self, schema_name, table_name, columns):
        """Add one or more TEXT columns to an existing PostgreSQL table."""
        query = f"ALTER TABLE \"{schema_name}\".\"{table_name}\" "
        column_parts = [f"ADD COLUMN \"{c}\" TEXT NULL " for c in columns]
        query += ", ".join(column_parts) + ";"
        self.execute_query(query)

    
    def insert_into(self, schema_name, table_name, data):
        """Insert a list of dicts into the given PostgreSQL table using parameterised queries."""
        db_data = [tuple(dic.values()) for dic in data]
        columns = ", ".join(["\"" + x + "\"" for x in data[0]])
        qm = ", ".join(["?"] * len(data[0]))
        query = f"INSERT INTO \"{schema_name}\".\"{table_name}\" ({columns}) VALUES ({qm})"
        self.execute_many(query, db_data)
    