using HtmlAgilityPack;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Elsevier.DownloadArticleMetadata
{
    class JournalScraper : IDisposable
    {
        private IWebDriver _driver;
        private string _outputDirectory;

        public JournalScraper(string outputDirectory)
        {
            _outputDirectory = outputDirectory;
        }

        public void RefreshBrowser()
        {
            Dispose();
            Console.WriteLine($"Initializing webdriver");
            _driver = CreateNewDriver();
        }

        public void GetJournalData(long id, string title)
        {
            List<string> articles = ProcessJournal(title);
            int i = 1;
            foreach (string articleLink in articles)
            {
                string articleInformation = GetArticleInformation(articleLink);
                if (!Directory.Exists(Path.Combine(_outputDirectory, title)))
                {
                    Directory.CreateDirectory(Path.Combine(_outputDirectory, title));
                }
                using StreamWriter sw = new StreamWriter(Path.Combine(_outputDirectory, title, $"{i}.json"));
                sw.WriteLine(articleInformation);
                sw.Flush();
                sw.Close();
                i++;
            }
        }

        private string GetArticleInformation(string link)
        {
            string url = $"https://sciencedirect.com/{link}";
            Console.WriteLine($"Reading article at: {url}");
    
            // Use the browser we already have open instead of a new web request
            _driver.Navigate().GoToUrl(url);
    
            // Give it a moment to load the JSON script tag
            Thread.Sleep(3000); 

            HtmlDocument doc = new HtmlDocument();
            doc.LoadHtml(_driver.PageSource);
    
            var node = doc.DocumentNode.SelectSingleNode("//script[@type = 'application/json' and @data-iso-key = '_0']");
    
            if (node == null)
            {
                throw new Exception("Could not find article JSON blob. The site might be blocking us or the layout changed.");
            }

            return node.InnerText;
        }

        private List<string> ProcessJournal(string journalName)
        {
            Console.WriteLine($"Processing: {journalName}");

            List<string> articleSubLinks = new List<string>();
            int offset = 0;
            int show = 25; // Has to be either 25, 50 or 100

            Boolean go = true;

            while (go)
            {
                var url = $"https://www.sciencedirect.com/search?pub={journalName.Replace(" ", "%20")}&show={show}&sortBy=date&offset={offset}&articleTypes=FLA";
                _driver.Navigate().GoToUrl(url);
                //_driver.Manage().Timeouts().PageLoad = new TimeSpan(0, 0, 10);
                Thread.Sleep(5000);
                HtmlDocument doc = new HtmlDocument();
                doc.LoadHtml(_driver.PageSource);
                var articles = doc.DocumentNode.SelectNodes("//a[contains(@class, 'result-list-title-link')]");

                if (articles is null || articles.Count == 0)
                {
                    Console.WriteLine("Got no results, so I assume I got everything.");
                    // Temporary debug: Save the page to see if it's a CAPTCHA
                    File.WriteAllText("debug_page.html", _driver.PageSource);
                    Console.WriteLine("Check debug_page.html to see if we were blocked.");
                    break;
                }
                Console.WriteLine($"Found {articles.Count} article(s)");
                foreach (var a in articles)
                {
                    string link = a.GetAttributeValue("href", string.Empty);
                    articleSubLinks.Add(link);
                }
                offset = offset + show;
                go = false;
            }
            return articleSubLinks;
        }

        public void Dispose()
        {
            Console.WriteLine("Disposing...");
            if (_driver != null)
                _driver.Close();
        }

        private IWebDriver CreateNewDriver()
        {
            Console.WriteLine("Create new webdriver using Selenium Manager");
    
            ChromeOptions options = new ChromeOptions();
            options.AddArgument("--headless=new"); 
            // Look more human.
            options.AddArgument("--disable-blink-features=AutomationControlled");
            options.AddExcludedArgument("enable-automation");
            options.AddAdditionalOption("useAutomationExtension", false);
            options.AddArgument("--window-size=1920,1080");
            options.AddArgument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");

            IWebDriver driver = new ChromeDriver(options);

            // Use JavaScript to remove the 'webdriver' flag entirely after launch
            ((IJavaScriptExecutor)driver).ExecuteScript("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})");
    
            driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(30);
            driver.Manage().Timeouts().PageLoad = TimeSpan.FromSeconds(30);

            return driver;
        }

    }
}
