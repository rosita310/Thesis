using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Helper;
using Database;
using System.Threading;
using System.Threading.Tasks;

namespace Elsevier.ParseOutput
{
    class Program
    {
        private static string schema = "elsevier";
        private static string propertiesFile = "../.env";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Started");
            Dictionary<string, string> properties = PropertiesReader.ReadProperties(propertiesFile);

            string user = properties["POSTGRES_USER"];
            string password = properties["POSTGRES_PASSWORD"];
            string database = properties["POSTGRES_DB"];
            string server = properties["POSTGRES_SERVER"];
            string dataStorage = properties["RAW_DATA"];
            string filepath = Path.Combine(properties["RAW_DATA"], properties["ELSEVIER_ARTICLE_JSON_SUBDIR"]);

            IDatabase db = DatabaseFactory.GetDatabase(DatabaseType.Postgres, user, password, database, server);
            
            Program p = new Program(db);

            await p.Execute(filepath);

            Console.WriteLine("Done.");

            Console.ReadKey();

        }

        private readonly IDatabase _database;

        private Program(IDatabase database)
        {
            _database = database;
        }

        private async Task Execute(string inputDir)
        {
            int maxConcurrency = 1;
            using(SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(maxConcurrency))
            {
                List<Task> tasks = new List<Task>();
                foreach (string file in Directory.EnumerateFiles(inputDir, "*.json", SearchOption.AllDirectories))
                {
                    concurrencySemaphore.Wait();

                    var t = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            ProcessFile(file);
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

        private readonly List<string> articleTags = new List<string>
        {
            "cid",
            "cover-date-start",
            "cover-date-text",
            "document-subtype",
            "document-type",
            "eid",
            "doi",
            "issuePii",
            "language",
            "pii",
            "srctitle",
            "suppl",
            "vol-first",
            "vol-iss-suppl-text",
            "issn",
            "issn-primary-formatted",
            "volRange",
            "titleString",
            "first-fp",
            "last-lp"
        };

        private void ProcessFile(string file)
        {
            string journal = Directory.GetParent(file).Name; // Path.GetDirectoryName(file);
            
            Console.WriteLine($"Processing: {file} ({journal})");
            DataTable articles = new DataTable();
            articles.Columns.Add(new DataColumn("$_id", typeof(int)));
            articles.Columns.Add(new DataColumn("$_journal", typeof(string)));
            foreach (var at in articleTags)
            {
                articles.Columns.Add(new DataColumn(at.Replace("-", "_"), typeof(string)));
            }

            DataTable datesDt = new DataTable();
            datesDt.Columns.Add(new DataColumn("$_id", typeof(int)));
            datesDt.Columns.Add(new DataColumn("$_journal", typeof(string)));
            datesDt.Columns.Add(new DataColumn("type", typeof(string)));
            datesDt.Columns.Add(new DataColumn("value", typeof(string)));

            int id = Int32.Parse(Path.GetFileNameWithoutExtension(file));
            // Console.WriteLine($"id: {id}");
            JObject obj = JObject.Parse(File.ReadAllText(file));
            JObject article = (JObject)obj["article"];

            DataRow dr = articles.NewRow();
            dr["$_id"] = id;
            dr["$_journal"] = journal;
            foreach (var t in articleTags)
            {
                var value = article[t]?.Value<string>();
                dr[t.Replace("-", "_")] = value;
            }

            articles.Rows.Add(dr);

            JObject dates = (JObject)article["dates"];
            foreach (var k in dates)
            {
                if (k.Key.Equals("Revised", StringComparison.OrdinalIgnoreCase))
                {
                    JArray revisedDates = (JArray)k.Value;
                    foreach (var rd in revisedDates)
                    {
                        DataRow dater = datesDt.NewRow();
                        dater["$_id"] = id;
                        dater["$_journal"] = journal;
                        dater["type"] = k.Key;
                        dater["value"] = rd.ToString();
                        datesDt.Rows.Add(dater);
                    }
                }
                else
                {
                    DataRow dater = datesDt.NewRow();
                    dater["$_id"] = id;
                    dater["$_journal"] = journal;
                    dater["type"] = k.Key;
                    dater["value"] = k.Value;
                    datesDt.Rows.Add(dater);
                }
            }

            // authors
            JObject authorsJson = (JObject)obj["authors"];
            (DataTable authors, DataTable refs, DataTable affiliations) = ReadAuthors(authorsJson, id, journal);

            WriteToDb(authors, "authors");
            WriteToDb(refs, "references");
            WriteToDb(affiliations, "affiliations");
            WriteToDb(datesDt, "dates");
            WriteToDb(articles, "articles");
            authors.Clear();
            refs.Clear();
            affiliations.Clear();
            datesDt.Clear();
            articles.Clear();
        }

        private void WriteToDb(DataTable dt, string targetTable)
        {
            Console.WriteLine("Writing to database");
            ulong rowCopied = _database.WriteToDb(schema, targetTable, dt);
            if (rowCopied != (ulong)dt.Rows.Count)
            {
                throw new Exception("Number of rows to write is not equal to written to database");
            }          
        }

        private (DataTable authors, DataTable refs, DataTable affiliations) ReadAuthors(JObject authorsJson, int article_id, string journal)
        {
            DataTable dtAuthors = new DataTable();
            dtAuthors.Columns.Add(new DataColumn("$_id", typeof(int)) { DefaultValue = article_id });
            dtAuthors.Columns.Add(new DataColumn("$_journal", typeof(string)) { DefaultValue = journal });
            dtAuthors.Columns.Add(new DataColumn("article_author_id", typeof(string)));
            dtAuthors.Columns.Add(new DataColumn("property", typeof(string)));
            dtAuthors.Columns.Add(new DataColumn("value", typeof(string)));

            DataTable refsDt = new DataTable();
            refsDt.Columns.Add(new DataColumn("$_id", typeof(int)) { DefaultValue = article_id });
            refsDt.Columns.Add(new DataColumn("$_journal", typeof(string)) { DefaultValue = journal });
            refsDt.Columns.Add(new DataColumn("article_author_id", typeof(string)));
            refsDt.Columns.Add(new DataColumn("refid", typeof(string)));
            refsDt.Columns.Add(new DataColumn("article_cross_ref_id", typeof(string)));

            DataTable affDt = new DataTable();
            affDt.Columns.Add(new DataColumn("$_id", typeof(int)) { DefaultValue = article_id });
            affDt.Columns.Add(new DataColumn("$_journal", typeof(string)) { DefaultValue = journal });
            affDt.Columns.Add(new DataColumn("affiliation_id", typeof(string)));
            affDt.Columns.Add(new DataColumn("property", typeof(string)));
            affDt.Columns.Add(new DataColumn("value", typeof(string)));

            JObject affiliations = (JObject)authorsJson["affiliations"];
            foreach(var affiliation in affiliations)
            {
                string affiliation_id = affiliation.Key;
                JObject affiliationJson = (JObject)affiliation.Value;
                JArray affiliationProperties = (JArray)affiliationJson["$$"];
                foreach (JObject property in affiliationProperties)
                {
                    string propLabel = property["#name"].Value<string>();
                    if (propLabel == "textfn")
                    {
                        DataRow dr = affDt.NewRow();
                        dr["affiliation_id"] = affiliation_id;
                        dr["property"] = propLabel;
                        string propValue = property["_"]?.Value<string>();
                        dr["value"] = propValue;
                        affDt.Rows.Add(dr);
                    }
                    else if (propLabel == "affiliation")
                    {
                        foreach (JObject p in (JArray)property["$$"])
                        {
                            DataRow dr = affDt.NewRow();
                            dr["affiliation_id"] = affiliation_id;
                            dr["property"] = p["#name"].Value<string>();
                            string propValue = p["_"]?.Value<string>();
                            dr["value"] = propValue;
                            affDt.Rows.Add(dr);
                        }
                    }
                }
            }

            JArray contentList = (JArray)authorsJson["content"];
            foreach (JObject content in contentList)
            {
                if (content["#name"].Value<string>() != "author-group") continue;
                JArray authorList = (JArray)content["$$"];
                foreach (JObject author in authorList)
                {
                    if (author["#name"].Value<string>() != "author") continue;
                    string article_author_id = author["$"]["id"].Value<string>();

                    string author_id = author["$"]["author-id"]?.Value<string>();
                    if (author_id != null)
                    {
                        DataRow dr = dtAuthors.NewRow();
                        dr["article_author_id"] = article_author_id;
                        dr["property"] = "author-id";
                        dr["value"] = author_id;
                        dtAuthors.Rows.Add(dr);
                    }

                    JArray authorProperties = (JArray)author["$$"];
                    foreach (JObject property in authorProperties)
                    {
                        if (property["#name"].Value<string>() == "cross-ref")
                        {
                            JObject cr = (JObject)property["$"];
                            DataRow dr = refsDt.NewRow();
                            dr["article_author_id"] = article_author_id;
                            dr["refid"] = cr["refid"].Value<string>();
                            dr["article_cross_ref_id"] = cr["id"]?.Value<string>();
                            refsDt.Rows.Add(dr);
                        }
                        else if (property["_"] == null) continue;
                        else
                        {
                            DataRow dr = dtAuthors.NewRow();
                            dr["article_author_id"] = article_author_id;
                            dr["property"] = property["#name"].Value<string>();
                            dr["value"] = property["_"]?.Value<string>();
                            dtAuthors.Rows.Add(dr);
                        }
                    }

                }
            } // End loop content
            return (dtAuthors, refsDt, affDt);
        }
    }
}
