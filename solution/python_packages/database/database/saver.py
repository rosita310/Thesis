"""
Saver helper class for auto-creating schemas/tables and writing data.

The Saver wraps a database backend (SqlServer or Postgress) and handles:
- Schema creation if it does not exist
- Table creation based on the shape of the data
- Column addition when new keys appear in the data
- Column size adjustment when values exceed the current column width (SQL Server only)
- Bulk insert of the data rows

Usage:
    saver = Saver(db)
    saver.save('my_schema', 'my_table', [{'col1': 'value1', 'col2': 'value2'}])
"""

class Saver:
    """Auto-creating database writer.

    Inspects the data on each call to save() and ensures the target table
    has the correct columns before inserting.

    Args:
        db: A database backend instance (SqlServer or Postgress).
    """

    def __init__(self, db):
        self.db = db


    def check_schema(self, schema_name):
        """Create the schema if it does not already exist."""
        if not self.db.schema_exists(schema_name):
            print("Schema does not exists -> creating")
            self.db.create_schema(schema_name)

    
    def equal_list_of_dicts(self, l) -> list:
        """Normalise a list of dicts so every dict has the same sorted set of keys.

        Missing keys are filled with None, and keys are sorted alphabetically so
        that the column order is consistent across rows.

        Args:
            l: List of dicts with potentially different keys.

        Returns:
            List of dicts all sharing the same sorted keys.
        """
        keys = []
        ret = []
        for d in l:
            for k in d:
                if k not in keys:
                    keys.append(k)
        for d in l:
            new_d = {}
            for k in sorted(keys):
                new_d[k] = d[k] if k in d else None
            ret.append(new_d)
        return ret


    def save(self, schema_name, table_name, data):
        """Save a list of dicts to the database, auto-creating schema/table as needed.

        For each column, the required size is determined by the longest value in the
        data. If the table already exists, new columns are added and (for SQL Server)
        undersized columns are widened. Then all rows are inserted.

        Args:
            schema_name: Target database schema.
            table_name:  Target table name.
            data:        List of dicts to insert. All dicts are normalised to the
                         same key set before writing.
        """
        data = self.equal_list_of_dicts(data)
        if len(data) == 0:
            return
        al = {}
        for row in data:
            for an in row:
                length = 1
                if row[an] is not None:
                    # Cast to str to safely call len() on non-string values (e.g. int)
                    length = len(row[an]) if len(row[an]) > 0 else 1
                if an not in al or al[an] <= length:
                    al[an] = length
        # print(al)

        if self.db.table_exists(schema_name, table_name):
            ec = self.db.get_column_info(schema_name, table_name)
            not_existent = {}
            to_adjust = {}
            for new_column in al:
                if new_column not in ec:
                    not_existent[new_column] = al[new_column]
                elif al[new_column] > ec[new_column]:
                    to_adjust[new_column] = al[new_column]
            
            if len(not_existent) > 0:
                print(f"adding {len(not_existent)} columns")
                self.db.add_columns(schema_name, table_name, not_existent)

            for c in to_adjust:
                print(f"adjusting column {c}")
                self.db.change_size(schema_name=schema_name, table_name=table_name, column_name=c, new_size=to_adjust[c])

        else:
            print("table does not exist")
            self.check_schema(schema_name)
            self.db.create_table(schema_name, table_name, al)

        # write the data    

        self.db.insert_into(schema_name, table_name, data)

        


