using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.IO.Enumeration;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Xml;
using System.Xml.Linq;
using Helper;
using Database;

namespace dblp.loader
{
    class Program
    {
        private static string schema = "dblp_dump";
        private static string propertiesFile = "../.env";


        [Obsolete]
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

            string filepath = Path.Combine(dataStorage, "dblp_xml_dump", "dblp.xml");

            string filepath = "solution/.net/DblpLoader/testfiles/test.xml";

            // p.ReadPublications(filepath);

            p.Execute(filepath);

            // string searchTerm = "https://doi.org/10.1007/978-3-540-36668-3_1";
            // long lineNumber =  p.Search(filepath, searchTerm);
            // if (lineNumber != 0)
            // {
            //     p.WriteRange(filepath, lineNumber, 100, "output2.xml");
            // }

            Console.WriteLine("Done, press any key to exit");
            Console.ReadKey();

        }

        private void WriteRange(string filePath, long lineNumber, int spread, string outputFile)
        {
            long readLineNumber = 0;
            long startNumber = lineNumber - spread;
            long endNumber = lineNumber + spread;
            using StreamReader sr = new StreamReader(filePath);
            using StreamWriter sw = new StreamWriter(outputFile);
            string line = String.Empty;
            while ((line = sr.ReadLine()) != null)
            {
                readLineNumber++;
                if (readLineNumber == startNumber)
                {
                    Console.WriteLine("Start writing");
                }
                if (readLineNumber >= startNumber && readLineNumber <= endNumber)
                {
                    sw.WriteLine(line);
                }
                if (readLineNumber > endNumber)
                {
                    sw.Flush();
                    Console.WriteLine("End writing");
                    break;
                }
            }
        }

        private long Search(string filepath, string searchTerm)
        {
            Console.Write($"Searching for: {searchTerm}... ");
            long lineNumber = 0;
            using StreamReader sr = new StreamReader(filepath);
            string line = String.Empty;
            while ((line = sr.ReadLine()) != null)
            {
                lineNumber++;
                if (line.Contains(searchTerm, StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"FOUND in '{line}' ({lineNumber})");
                    return lineNumber;
                }
            }
            Console.WriteLine("NOT FOUND");
            return 0;
        }

        private readonly IDatabase _database;

        private Program(IDatabase database)
        {
            _database = database;
        }

        private void Execute(string filepath)
        {
            long id = 0;
            foreach (XElement element in ReadElements(filepath))
            {
                id++;
                List<Dictionary<string, string>> sets = ProcessElement(element, id);
                ConvertSets(sets);
                if (id % 100000 == 0)
                {
                    Console.WriteLine($"Processed {id} elements");
                    DumpDataSetsToDb();
                }
            }
            DumpDataSetsToDb();
        }

        private IEnumerable<XElement> ReadElements(string filePath)
        {
            XmlReaderSettings settings = new XmlReaderSettings()
            {
                DtdProcessing = DtdProcessing.Parse,
                ValidationType = ValidationType.DTD,
                XmlResolver = new XmlUrlResolver()
            };
            using (XmlReader reader = XmlReader.Create(filePath, settings))
            {
                reader.MoveToContent();
                while (!reader.EOF)
                {
                    if (reader.NodeType == XmlNodeType.Element && reader.Depth == 1)
                    {
                        XElement el = XElement.ReadFrom(reader) as XElement;
                        if (el != null)
                        {
                            yield return el;
                        }
                    }
                    else
                    {
                        reader.Read();
                    }
                }
            }
            yield break;
        }

        private void DumpDataSetsToDb()
        {
            foreach (KeyValuePair<string, DataTable> kv in tables)
            {
                if (kv.Value.Rows.Count > 0)
                {
                    ulong rowCopied = _database.WriteToDb(schema, kv.Key, kv.Value);
                    if (rowCopied != (ulong)kv.Value.Rows.Count)
                    {
                        throw new Exception("Number of rows to write is not equal to written to database");
                    }
                }
                kv.Value.Clear();
            }
        }
        

        private List<Dictionary<string, string>> ProcessElement(XElement element, long id, long parentObjectId = 0, string parentObjectType = null)
        {
            //Console.WriteLine(element.Name);
            List<Dictionary<string, string>> sets = new List<Dictionary<string, string>>();
            Dictionary<string, string> values = new Dictionary<string, string>();
            values["$_object_type"] = element.Name.ToString();
            values["$_object_id"] = id.ToString();

            if (parentObjectId != 0)
            {
                values["$_parent_object_id"] = parentObjectId.ToString();
                values["$_parent_object_type"] = parentObjectType.ToString();
            }
            foreach (XAttribute attr in element.Attributes())
            {
                values[attr.Name.ToString()] = attr.Value.ToString();
            }
            sets.Add(values);
            if (element.Name.ToString().Equals("title", StringComparison.OrdinalIgnoreCase))
            {
                values[element.Name.ToString()] = element.Value.ToString();
            }
            else if (element.HasElements)
            {
                long subId = 0;
                foreach (XElement subElement in element.Elements())
                {
                    subId++;
                    sets.AddRange(ProcessElement(subElement, subId, id, element.Name.ToString()));
                }
            }
            else
            {
                values[element.Name.ToString()] = element.Value.ToString();
            }
            return sets;
        }

        private Dictionary<string, DataTable> tables = new Dictionary<string, DataTable>();

        private void ConvertSets(List<Dictionary<string, string>> sets)
        {
            foreach (Dictionary<string, string> dict in sets)
            {
                string objectType = dict["$_object_type"];
                if (!tables.ContainsKey(objectType))
                {
                    tables.Add(objectType, new DataTable());
                }
                DataTable dt = tables[objectType];
                foreach (string key in dict.Keys)
                {
                    if (!dt.Columns.Contains(key))
                    {
                        Type type;
                        if (key.Equals("$_object_id") || key.Equals("$_parent_object_id"))
                            type = typeof(int);
                        else
                            type = typeof(string);
                        
                        DataColumn dataColumn = new DataColumn(key, type);
                        dt.Columns.Add(dataColumn);
                    }
                }
                DataRow dr = dt.NewRow();
                foreach (KeyValuePair<string, string> kv in dict)
                {
                    if (kv.Key.Equals("$_object_id") || kv.Key.Equals("$_parent_object_id"))
                        dr[kv.Key] = int.Parse(kv.Value);
                    else
                        dr[kv.Key] = kv.Value;

                }
                dt.Rows.Add(dr);
            }
        }

    }
}
