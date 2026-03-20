using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using Helper;
using Database;
using System.Data;

namespace Elsevier.DownloadArticleMetadata
{
    class Program
    {
        private static int _retryCount = 5;

        private static readonly string propertiesFile = "../.env";

        private static IDatabase db;

        static void Main(string[] args)
        {
            Console.WriteLine("Starting");

            Dictionary<string, string> properties = PropertiesReader.ReadProperties(propertiesFile);
            string savedir = Path.Combine(properties["RAW_DATA"], properties["ELSEVIER_ARTICLE_JSON_SUBDIR"]);

            string user = properties["POSTGRES_USER"];
            string password = properties["POSTGRES_PASSWORD"];
            string database = properties["POSTGRES_DB"];
            string server = properties["POSTGRES_SERVER"];

            db = DatabaseFactory.GetDatabase(DatabaseType.Postgres, user, password, database, server);


            JournalScraper js = new JournalScraper(savedir);
            js.RefreshBrowser();

            while (true)
            {
                (long id, string title) = GetJournal();
                if (id == 0) break;
                int retryCounter = 0;
                Console.WriteLine($"Start processing journal {id} ({title})");
                bool succeeded = false;
                while (!succeeded && retryCounter < _retryCount)
                {
                    retryCounter++;
                    
                    try
                    {
                        js.GetJournalData(id, title);
                        Console.WriteLine($"Attempt {retryCounter} succeeded");
                        succeeded = true;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine($"Attempt {retryCounter} failed");
                        js.RefreshBrowser();
                    }
                }
                if (!succeeded)
                    Console.WriteLine($"FAILED processing journal {id} ({title})");
                else
                    Console.WriteLine($"SUCCEEDED processing journal {id} ({title})");
            }
            Console.WriteLine("Done");
        }

        static (long, string) GetJournal()
        {
            string sql = "UPDATE elsevier.article_metadata_input " +
                         "SET status = 'RUNNING' " +
                         "WHERE id = (" +
                         "SELECT MIN(id) " +
                         "FROM " +
                         "elsevier.article_metadata_input " +
                         "WHERE status = 'TODO') " +
                         "RETURNING id, title";
            DataTable dt = db.GetData(sql);
            if (dt.Rows.Count == 0) { return (0, ""); }
            DataRow row = dt.Rows[0];
            long id = (long)row["id"];
            string title = row["title"].ToString();

            return (id, title);
        }
    }
}
