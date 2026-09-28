import database
import logging

class Saver:

    def __init__(self, db):
        self.db = db


    def check_schema(self, schema_name):
        if not self.db.schema_exists(schema_name):
            self.db.create_schema(schema_name)


    def equal_list_of_dicts(self, l) -> list:
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
        data = self.equal_list_of_dicts(data)
        if len(data) == 0:
            return
        al = {}
        for row in data:
            for an in row:
                length = 1
                if row[an] is not None:
                    length = len(row[an])
                if an not in al or al[an] <= length:
                    al[an] = length

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
                #print(f"adding {len(not_existent)} columns")
                query = f"ALTER TABLE [{schema_name}].[{table_name}] ADD "
                column_parts = [f"[{c}] NVARCHAR({not_existent[c]}) NULL " for c in not_existent]
                query += ", ".join(column_parts) + ";"
                self.db.execute_query(query)

            for c in to_adjust:
                #print(f"adjusting column {c}")
                query = f"ALTER TABLE [{schema_name}].[{table_name}] ALTER COLUMN [{c}] NVARCHAR({to_adjust[c]}) NULL "
                self.db.execute_query(query)

        else:
            #print("table does not exist")
            self.check_schema(schema_name)
            self.db.create_table(schema_name, table_name, al)

        # write the data
        
        db_data = [tuple(dic.values()) for dic in data]

        columns = ", ".join(["[" + x + "]" for x in data[0]])
        qm = ", ".join(["?"] * len(data[0]))
        query = f"INSERT INTO [{schema_name}].[{table_name}] ({columns}) VALUES ({qm})"

        self.db.execute_many(query, db_data)

        


