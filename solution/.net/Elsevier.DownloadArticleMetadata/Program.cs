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

        private static readonly string propertiesFile = GetConfigPath();

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

            InitializeQueue();

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

        private static void InitializeQueue()
        {
            Console.WriteLine("Syncing journal queue...");

            // Fetch journals from the Python scraper's table
            DataTable journalsFromSource = db.GetData("SELECT title FROM elsevier.journals");
            if (journalsFromSource.Rows.Count == 0) return;

            // Check if the queue table exists and get existing titles
            HashSet<string> existingTitles = new HashSet<string>();
            try 
            {
                DataTable existingQueue = db.GetData("SELECT title FROM elsevier.article_metadata_input");
                foreach (DataRow row in existingQueue.Rows) 
                    existingTitles.Add(row["title"].ToString());
            }
            catch { /* Table doesn't exist yet, GetData will throw an exception */ }

            // Prepare only the NEW journals
            DataTable syncTable = new DataTable();
            syncTable.Columns.Add("title", typeof(string));
            syncTable.Columns.Add("status", typeof(string));

            foreach (DataRow sourceRow in journalsFromSource.Rows)
            {
                string title = sourceRow["title"].ToString();
                if (!existingTitles.Contains(title))
                    syncTable.Rows.Add(title, "TODO");
            }

            if (syncTable.Rows.Count > 0)
            {
                // Use WriteToDb to create/update the table
                // This will create 'title' and 'status' columns.
                db.WriteToDb("elsevier", "article_metadata_input", syncTable);

                // Ensure the 'id' SERIAL column exists
                // Even though GetData expects a result, it will still execute this command.
                try 
                {
                    // IF NOT EXISTS is vital so we don't error out on the second run
                    db.GetData("ALTER TABLE elsevier.article_metadata_input ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY");
                } 
                catch (Exception ex) 
                { 
                    Console.WriteLine("Note: ID column already present or handled. Exception:"); 
                    Console.WriteLine(ex.ToString());
                }
            }
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
            long id = Convert.ToInt64(row["id"]);
            string title = row["title"].ToString();

            return (id, title);
        }

        private static string GetConfigPath()
        {
            // Start at the folder where the application is currently executing
            DirectoryInfo currentDir = new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory);

            // Walk up the folder tree until we find the "solution" folder
            while (currentDir != null && currentDir.Name != "solution")
            {
                currentDir = currentDir.Parent;
            }

            if (currentDir == null)
            {
                throw new DirectoryNotFoundException("Could not find the 'solution' directory in the path hierarchy.");
            }

            // Combine the path of the solution folder with the config file name
            return Path.Combine(currentDir.FullName, "config.env");
        }
    }
}
