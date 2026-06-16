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
        int pagesToScrape = 1; // For now, we will only scrape the first page of results for each journal. We can increase this later if we want more articles.

        private IWebDriver _driver;
        private string _outputDirectory;

        private Random _random = new Random();

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
            // We will only do one page for now, but we can loop through them if we want to get more articles.
            int page = 1;

            while (page <= pagesToScrape)
            {
                var url = $"https://www.sciencedirect.com/search?pub={journalName.Replace(" ", "%20")}&show={show}&sortBy=date&offset={offset}&articleTypes=FLA";
                _driver.Navigate().GoToUrl(url);
                // CAPTCHA WAITER 
                bool pageReady = false;
                int maxWaitAttempts = 60; // 60 attempts * 2 seconds = 2 minutes max wait per journal
                int attempts = 0;

                HtmlDocument doc = new HtmlDocument();
                while (!pageReady && attempts < maxWaitAttempts)
                {
                    doc.LoadHtml(_driver.PageSource);
        
                    // 1. Check if the Captcha Wiggle box or Challenge elements exist
                    bool hasCaptcha = doc.DocumentNode.SelectSingleNode("//div[@id='captcha-box']|//iframe[contains(@src, 'arkose')]|//div[contains(@id, 'px-container')]") != null 
                          || _driver.PageSource.Contains("captcha-box") 
                          || _driver.Title.Contains("Access Denied")
                          || _driver.Title.Contains("Pardon Our Interruption");

                    // 2. Check if the actual data has successfully loaded instead
                    var tempArticles = doc.DocumentNode.SelectNodes("//a[contains(@class, 'result-list-title-link')]");
                    bool hasData = tempArticles != null && tempArticles.Count > 0;

                    if (hasCaptcha)
                    {
                        // Play a sound to alert you
                        Console.Beep(800, 300); 
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"[ALERT] Captcha detected for '{journalName}'! Please solve it in the browser window now...");
                        Console.ResetColor();
            
                        Thread.Sleep(3000); // Wait 3 seconds before checking again to give you time
                        attempts++;
                    }
                    else if (hasData)
                    {
                        // Data is here! We can safely proceed.
                        pageReady = true;
                    }
                    else
                    {
                        // The page might just be loading slowly normally
                        Thread.Sleep(2000);
                        attempts++;
                    }
                }
                var articles = doc.DocumentNode.SelectNodes("//a[contains(@class, 'result-list-title-link')]");

                if (articles is null || articles.Count == 0)
                {
                    Console.WriteLine("Got no results, so I assume I got everything.");
                    break;
                }
                Console.WriteLine($"Found {articles.Count} article(s)");
                foreach (var a in articles)
                {
                    string link = a.GetAttributeValue("href", string.Empty);
                    articleSubLinks.Add(link);
                }
                offset = offset + show;
                page++;
                Thread.Sleep(1000 + _random.Next(3000)); // Don't send requests too quickly, add some variance.
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
            Console.WriteLine("Initializing stealth WebDriver context...");
    
            ChromeOptions options = new ChromeOptions(); 
    
            // 1. Core Automation Cloaking
         options.AddArgument("--disable-blink-features=AutomationControlled");
            options.AddExcludedArgument("enable-automation");
            options.AddAdditionalOption("useAutomationExtension", false);
    
            // 2. Clear Network Fingerprint Anomalies (The Triggers)
            // Firewalls look for specific command-line flags that only bots use
            options.AddArgument("--disable-infobars");
            options.AddArgument("--disable-browser-side-navigation");
            options.AddArgument("--disable-gpu"); // Helps normalize the canvas fingerprinting footprint
    
            // 3. Set a highly realistic human screen footprint
            options.AddArgument("--window-size=1920,1080");
            options.AddArgument("--start-maximized");

            // 4. Force a highly authentic, native Chrome 148 User-Agent string
            // A generic string or missing minor version triggers immediate firewall dropping
            options.AddArgument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36");

            // 5. Isolated Project Profile Path Setup
            string projectFolder = AppDomain.CurrentDomain.BaseDirectory;
            string customProfilePath = Path.Combine(projectFolder, "ChromeThesisProfile");
            options.AddArgument($"--user-data-dir={customProfilePath}");

            // 6. Instantiate the Driver with Extended Startup Timeout
            ChromeDriverService service = ChromeDriverService.CreateDefaultService();
            IWebDriver driver = new ChromeDriver(service, options, TimeSpan.FromSeconds(90));

            // 7. Dynamic Webdriver Property Nullification
            try
            {
                ((IJavaScriptExecutor)driver).ExecuteScript(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});" +
                    "window.chrome = { runtime: {} };" + // Emulate native Chrome runtime properties
                    "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});" // Normalize language arrays
                );
            }
            catch { /* Safe fallback */ }
    
            driver.Manage().Timeouts().ImplicitWait = TimeSpan.FromSeconds(30);
            driver.Manage().Timeouts().PageLoad = TimeSpan.FromSeconds(30);

            return driver;
        }

    }
}
