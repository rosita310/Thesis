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
using Microsoft.VisualBasic.FileIO;
using System.Threading.Tasks;
using System.Threading;

namespace dblp.loader
{
    class Program
    {
        private static string schema = "opencitations_dump";
        private static string propertiesFile = "../.env";


        [Obsolete]
        static async Task Main(string[] args)
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

            string filepath = Path.Combine(dataStorage, "opencitations");

            await p.Execute(filepath);

            Console.WriteLine("Done, press any key to exit");
            Console.ReadKey();

        }


        private readonly IDatabase _database;

        private Program(IDatabase database)
        {
            _database = database;
        }

        private String[] GetLncsDoi() 
        {
            Console.WriteLine("Getting doi's from lncs chapters");
            string query = "SELECT TRIM(REPLACE(doi, 'https://doi.org/', '')) as doi FROM springer_lncs.chapter;";
            DataTable data = _database.GetData(query);
            String[] dois = data.AsEnumerable().Select(x => x[0].ToString()).ToArray();
            return dois;
        }

        private async Task Execute(string filepath)
        {
            String[] doisString = GetLncsDoi();
            var dois = new HashSet<string>(doisString);
            List<string> files = Directory.EnumerateFiles(filepath, "*.csv", System.IO.SearchOption.AllDirectories).ToList();
            int totalFiles = files.Count;
            int fileNumber = 0;
            int maxConcurrency = 8;

            using(SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(maxConcurrency))
            {
                List<Task> tasks = new List<Task>();
                foreach(var file in files)
                {
                    concurrencySemaphore.Wait();

                    var t = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            ProcessFile(file, dois);
                        }
                        finally
                        {
                            concurrencySemaphore.Release();
                        }
                    });

                    tasks.Add(t);
                }
                Console.WriteLine("Waiting for tasks to finish");
                Task.WaitAll(tasks.ToArray());
            }
        }

        private void ProcessFile(string file, HashSet<string> dois)
        {
            string fileName = Path.GetFileName(file);
            Console.WriteLine($"Processing file {fileName}");

            bool firstRow = true;
            long processedFromFile = 0;

            DataTable dt = null;

            using (TextFieldParser csvParser = new TextFieldParser(file))
            {
                csvParser.CommentTokens = new string[] { "#" };
                csvParser.SetDelimiters(new string[] { "," });
                csvParser.HasFieldsEnclosedInQuotes = true;

                long lineNumber = 0;

                while (!csvParser.EndOfData)
                {
                    lineNumber++;
                    if (firstRow)
                    {
                        string[] columnNames = csvParser.ReadFields();
                        dt = new DataTable();
                        foreach (string cn in columnNames)
                        {
                            dt.Columns.Add(new DataColumn(cn.Trim(), typeof(string)));
                        }
                        DataColumn dc  = new DataColumn("$_rec_src", typeof(string)) { DefaultValue = fileName };
                        dt.Columns.Add(dc);
                        firstRow = false;
                    }
                    else
                    {
                        string[] lineValues = csvParser.ReadFields();
                        if (dois.Contains(lineValues[1]))
                        {
                            DataRow dr = dt.NewRow();
                            for (int i = 0; i < lineValues.Length; i++)
                            {
                                dr[i] = lineValues[i].Trim();
                            }
                            dt.Rows.Add(dr);
                            processedFromFile++;
                        }
                    }
                    if (lineNumber % 100000 == 0)
                    {
                        Console.WriteLine($"{fileName} Still running (linenumber: {lineNumber})...");
                    }

                    if (processedFromFile > 0 && processedFromFile % 100000 == 0)
                    {
                        WriteToDb(dt);
                        dt.Clear();
                    }
                }
            }
            WriteToDb(dt);
            dt.Clear();
            Console.WriteLine($"{fileName} Done ({processedFromFile} interesting references)");
        }

        private void WriteToDb(DataTable dt)
        {
            Console.WriteLine("Writing to database");
            ulong rowCopied = _database.WriteToDb(schema, "reference", dt);
            if (rowCopied != (ulong)dt.Rows.Count)
            {
                throw new Exception("Number of rows to write is not equal to written to database");
            }          
        }

    }
}
