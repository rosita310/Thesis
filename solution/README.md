# Solution

## .env file

In deze directory moet een file met de naam '.env' staan.  
De opzet van de file kun je vinden in 'env-example'.  
Vervang de waardes (inclusief de '<' en '>').

## Folder structure:

<pre>
<b>solution</b>
<b>├ .net:</b> .Net applications
<b>  ├ AminerLoader:</b> Software to load an Aminer file in the database.
<b>  ├ Database:</b> Separate library to handle database interaction.
<b>  ├ DblpLoader:</b> Software to load the DBLP XML file.
<b>  ├ Elsevier.ParseOutput:</b> Software to parse scraped JSON files from Elsevier.
<b>  └ Helper:</b> Library to read the properties from the .env file.
<b>├ acm:</b> Python scipts to download and parse the editorial boards from ACM.
<b>├ analysis:</b> Please skip :) (Tried what was possible with R).
<b>├ core_scraper:</b> Software to scrape data from CORE.
<b>├ database:</b> Docker image for a postgres database. Not used in the project.
<b>├ dblp_api:</b> Python scripts to get data from DBLP API.
<b>├ integration:</b> Database project (dbt).
<b>├ lncs:</b> Python scripts for Springer LNCS
<b>  ├ front_matter_download:</b> Scripts to download the Front Matter.
<b>  └ scraper:</b> Scripts to scrape data from Springer LNCS.
<b>├ pdf_parser:</b> Java application to parse LNCS Front Matter documents.
<b>└ python_packages:</b> Generic packages used in other python projects.
</pre>


### Database

Dit is een docker file van een postgres database gebruikt voor lokale 
ontwikkeling. Noodzakelijk hiervoor is dat docker en docker-compose 
geinstalleerd zijn.

## Tools used

VSCode: For most projects, except pdf_parser (Intellij), and Elsevier download tooles (Visual studio)  
Intellij: For pdf_parser.  
Azure Data Studio: For querying.