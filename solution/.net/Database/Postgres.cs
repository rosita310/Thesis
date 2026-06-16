using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Database
{
    class Postgres : Generic, IDatabase
    {
        private readonly string _user;
        private readonly string _password;
        private readonly string _database;
        private readonly string _server;

        public Postgres(string user, string password, string database, string server)
        {
            _user = user;
            _password = password;
            _database = database;
            _server = server;
        }

        private NpgsqlConnection GetConnection()
        {
            string connectionString = $"Host={_server};Username={_user};Password={_password};Database={_database};ssl mode=Disable;Trust Server Certificate=true;";
            NpgsqlConnection connection = new NpgsqlConnection(connectionString);
            return connection;
        }

        public override ulong WriteToDb(string schemaName, string tableName, DataTable dt)
        {
            bool schemaExists = SchemaExists(schemaName);
            if (!schemaExists)
            {
                CreateSchema(schemaName);
            }
            Dictionary<string, int> sourceColumns = DataTableToColumns(dt);

            if (!TableExists(schemaName, tableName))
            {
                string createStmt = GetCreateTableStatement(dt, schemaName, tableName);
                ExecuteNonQuery(createStmt);
            }
            else
            {
                
                List<string> columnsToAdd = new List<string>();
                // check if we need to change
                List<string> columnsFromStaging = GetColumnInfo(schemaName, tableName);
                foreach (KeyValuePair<string, int> dtColumn in sourceColumns)
                {
                    if (!columnsFromStaging.Contains(dtColumn.Key))
                    {
                        columnsToAdd.Add(dtColumn.Key);
                        continue;
                    }
                }
                // Voeg kolommen toe als die nodig zijn.
                if (columnsToAdd.Count > 0)
                {
                    Console.WriteLine($"Adding {columnsToAdd.Count} columns");
                    StringBuilder qry = new StringBuilder();
                    qry.AppendLine($"ALTER TABLE \"{schemaName}\".\"{tableName}\" ");
                    List<string> columnAdd = new List<string>();
                    foreach (string c in columnsToAdd)
                    {
                        columnAdd.Add($"ADD COLUMN \"{c}\" TEXT NULL");
                    }
                    qry.AppendLine(string.Join(", " + Environment.NewLine, columnAdd));
                    qry.AppendLine(";");
                    ExecuteNonQuery(qry.ToString());
                }
            }

            return CopyData(schemaName, tableName, dt);

        }

        private ulong CopyData(string schemaName, string tableName, DataTable dt)
        {
            List<string> columns = new List<string>();
            foreach (DataColumn sc in dt.Columns)
            {
                columns.Add($"{sc.ColumnName}");
            }
            string columnNames = string.Join(", " + Environment.NewLine, columns.Select(c => '\"' + c + '\"'));

            using var connection = GetConnection();
            connection.Open();

            string copySatement = $"COPY {schemaName}.{tableName} ({columnNames}) FROM STDIN (FORMAT BINARY)";
            using var writer = connection.BeginBinaryImport(copySatement);
            foreach (DataRow dr in dt.Rows)
            {
                writer.StartRow();
                foreach (string c in columns)
                {
                    string value = dr[c].ToString().Replace("\0", "");
                    writer.Write(value, NpgsqlTypes.NpgsqlDbType.Text);
                }
            }
            ulong result = writer.Complete();
            Console.WriteLine($"Wrote {result} rows to {tableName}");
            return result;
        }

        private List<string> GetColumnInfo(string schemaName, string tableName)
        {
            string query = "SELECT COLUMN_NAME " +
                "FROM information_schema.columns " +
                $"WHERE table_schema = '{schemaName}' " +
                $"AND table_name = '{tableName}'";
            List<string> columns = new List<string>();
            using var connection = GetConnection();
            connection.Open();
            using var command = new NpgsqlCommand(query, connection);
            using NpgsqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                columns.Add(reader.GetString(0));
            }
            return columns;
        }

        public override DataTable GetData(string query)
        {
            DataTable dt = new DataTable();
            using var connection = GetConnection();
            connection.Open();
            using var command = new NpgsqlCommand(query, connection);
            using NpgsqlDataAdapter da = new NpgsqlDataAdapter(command);
            da.Fill(dt);
            return dt;
        }

        private string GetCreateTableStatement(DataTable dt, string schemaName, string tableName)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE {schemaName}.{tableName} (");
            List<string> columnParts = new List<string>();
            foreach (DataColumn dc in dt.Columns)
            {
                string columnPart = $"\"{dc.ColumnName}\" TEXT NULL";
                columnParts.Add(columnPart);
            }
            string s = string.Join(", " + Environment.NewLine, columnParts);
            sb.AppendLine(s);
            sb.AppendLine(");");
            return sb.ToString();
        }

        private bool SchemaExists(string schemaName)
        {
            string query = "SELECT schema_name " +
                "FROM information_schema.schemata " +
                $"WHERE schema_name = '{schemaName}'";
            return ExecuteBool(query);
        }

        private void CreateSchema(string schemaName)
        {
            string query = $"CREATE SCHEMA {schemaName}";
            ExecuteNonQuery(query);
        }

        private bool TableExists(string schemaName, string tableName)
        {
            string query = "SELECT 1 " +
                "FROM information_schema.tables " +
                $"WHERE table_schema = '{schemaName}' " +
                $"AND table_name = '{tableName}'";
            return ExecuteBool(query);
        }

        private bool ExecuteBool(string query)
        {
            using var connection = GetConnection();
            connection.Open();
            using var command = new NpgsqlCommand(query, connection);
            var result = command.ExecuteScalar();
            bool value = result != null && result != DBNull.Value;
            return value;
        }

        private void ExecuteNonQuery(string query)
        {
            using var connection = GetConnection();
            connection.Open();
            using var command = new NpgsqlCommand(query, connection);
            command.ExecuteNonQuery();
        }
    }
}
