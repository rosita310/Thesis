using Database;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Helper;

namespace Aminer
{
    class Program
    { 

        private static string schemaName = "aminer";
        private static string propertiesFile = "../.env";

        static void Main(string[] args)
        {
            Console.WriteLine("Program started");

            Dictionary<string, string> properties = PropertiesReader.ReadProperties(propertiesFile);

            string user = properties["POSTGRES_USER"];
            string password = properties["POSTGRES_PASSWORD"];
            string database = properties["POSTGRES_DB"];
            string server = properties["POSTGRES_SERVER"];
            string dataStorage = properties["RAW_DATA"];

            IDatabase db = DatabaseFactory.GetDatabase(DatabaseType.Postgres, user, password, database, server);
            
            Program p = new Program(db);

            string filepath = Path.Combine(dataStorage, "aminer", "dblp.v12.json");

            p.Execute(filepath);

            Console.WriteLine("Program ended");
            Console.ReadKey();
        }

        private readonly IDatabase _database;

        private Program(IDatabase database)
        {
            _database = database;
        }

        private void Test()
        {
            _database.WriteToDb(schemaName, null, null);
        }

        private void Execute(string filepath)
        {
            string filename = Path.GetFileName(filepath);
            Dictionary<string, DataTable> dataCache = BuildDatasets();
            using StreamReader sr = new StreamReader(filepath);
            long linesRead = 0;
            string line = string.Empty;
            while ((line = sr.ReadLine()) != null)
            {
                JObject obj = ParseLine(line);
                if (obj == null)
                {
                    continue;
                }
                DataRow paperRow = GetPaper(obj, dataCache["paper"]);
                paperRow["$_source_file"] = filename;
                dataCache["paper"].Rows.Add(paperRow);
                foreach (DataRow dr in GetAuthors(obj, dataCache["author"]))
                {
                    dr["$_source_file"] = filename;
                    dataCache["author"].Rows.Add(dr);
                }
                foreach (DataRow dr in GetAuthorPapers(obj, dataCache["author_paper"]))
                {
                    dr["$_source_file"] = filename;
                    dataCache["author_paper"].Rows.Add(dr);
                }
                foreach (DataRow dr in GetReferences(obj, dataCache["reference"]))
                {
                    dr["$_source_file"] = filename;
                    dataCache["reference"].Rows.Add(dr);
                }
                linesRead++;
                if (linesRead % 100000 == 0)
                {
                    Console.WriteLine($"Total lines read: {linesRead}");
                    foreach (KeyValuePair<string, DataTable> kv in dataCache)
                    {
                        _database.WriteToDb(schemaName, kv.Key, kv.Value);
                        kv.Value.Clear();
                    }
                }
            }
            Console.WriteLine($"Total lines read: {linesRead}");
            foreach (KeyValuePair<string, DataTable> kv in dataCache)
            {
                _database.WriteToDb(schemaName, kv.Key, kv.Value);
                kv.Value.Clear();
            }
        }


        private IEnumerable<DataRow> GetReferences(JObject obj, DataTable dataTable)
        {
            List<DataRow> returnValues = new List<DataRow>();
            JArray references = (JArray)obj["references"];
            if (references != null)
            {
                foreach (JToken r in references.Children())
                {
                    DataRow dr = dataTable.NewRow();
                    dr["from_paper_id"] = obj["id"];
                    dr["to_paper_id"] = r.ToString();
                    returnValues.Add(dr);
                }
            }
            return returnValues;
        }

        private IEnumerable<DataRow> GetAuthorPapers(JObject obj, DataTable dt)
        {
            List<DataRow> returnValues = new List<DataRow>();
            JArray authors = (JArray)obj["authors"];
            if (authors != null)
            {
                foreach (JObject a in authors.Children())
                {
                    DataRow dr = dt.NewRow();
                    dr["paper_id"] = obj["id"];
                    dr["author_id"] = a["id"];
                    returnValues.Add(dr);
                }
            }
            return returnValues;
        }

        private IEnumerable<DataRow> GetAuthors(JObject obj, DataTable dt)
        {
            List<DataRow> returnValues = new List<DataRow>();
            JArray authors = (JArray)obj["authors"];
            if (authors != null)
            {
                foreach (JObject a in authors.Children())
                {
                    DataRow dr = dt.NewRow();
                    foreach (string columnName in _authorColums.Keys)
                    {
                        dr[columnName] = a[columnName];
                    }
                    returnValues.Add(dr);
                }
            }
            return returnValues;
        }

        private DataRow GetPaper(JObject obj, DataTable dt)
        {
            DataRow dr = dt.NewRow();
            foreach (string columnName in _paperColums.Keys)
            {
                if (columnName.StartsWith("venue"))
                {
                    if (obj["venue"] != null)
                    {
                        string key = columnName.Substring(6);
                        if (obj["venue"][key] != null)
                        {
                            dr[columnName] = obj["venue"][key];
                        }
                    }
                }
                else 
                { 
                    dr[columnName] = obj[columnName]; 
                }
            }
            return dr;
        }

        private Dictionary<string, Type> _paperColums = new Dictionary<string, Type>
        {
            { "id", typeof(string) },
            { "title", typeof(string) },
            { "year", typeof(string) },
            { "n_citation", typeof(int) },
            { "page_start", typeof(string) },
            { "page_end", typeof(string) },
            { "doc_type", typeof(string) },
            { "lang", typeof(string) },
            { "publisher", typeof(string) },
            { "volume", typeof(string) },
            { "issue", typeof(string) },
            { "issn", typeof(string) },
            { "isbn", typeof(string) },
            { "doi", typeof(string) },
            { "venue_id", typeof(long) },
            { "venue_raw", typeof(string) },
            { "$_source_file", typeof(string) }

        };

        private Dictionary<string, Type> _authorColums = new Dictionary<string, Type>
        {
            { "id", typeof(string) },
            { "name", typeof(string) },
            { "org", typeof(string) },
            { "$_source_file", typeof(string) }
        };
        

        private Dictionary<string, DataTable> BuildDatasets()
        {
            Dictionary<string, DataTable> returnSet = new Dictionary<string, DataTable>();
                     
            returnSet.Add("paper", BuildDataTable(_paperColums));
            returnSet.Add("author", BuildDataTable(_authorColums));

            Dictionary<string, Type> authorPaperColums = new Dictionary<string, Type>
            {
                { "paper_id", typeof(string) },
                { "author_id", typeof(string) },
                { "$_source_file", typeof(string) }
            };
            returnSet.Add("author_paper", BuildDataTable(authorPaperColums));

            Dictionary<string, Type> referenceColums = new Dictionary<string, Type>
            {
                { "from_paper_id", typeof(string) },
                { "to_paper_id", typeof(string) },
                { "$_source_file", typeof(string) }
            };
            returnSet.Add("reference", BuildDataTable(referenceColums));

            return returnSet;
        }

        private DataTable BuildDataTable(Dictionary<string, Type> columns)
        {
            DataTable dataTable = new DataTable();
            foreach (KeyValuePair<string, Type> kv in columns)
            {
                dataTable.Columns.Add(new DataColumn(kv.Key, kv.Value) { DefaultValue = DBNull.Value });
            }
            return dataTable;
        }

        private JObject ParseLine(string line)
        {
            line = line.Trim();
            if (line.StartsWith(","))
            {
                line = line.Substring(1);
                return JObject.Parse(line);
            }
            else if (line.StartsWith("[") || line.StartsWith("]"))
            {
                return null;
            }
            else
            {
                return JObject.Parse(line);
            }
        }
    }
}
