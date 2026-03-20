import program
from saver import Saver
import database
import logging

logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')


config = program.read_config('../.env')
db = database.Postgress(
    server=config['POSTGRES_SERVER'], 
    database=config['POSTGRES_DB'],
    user=config['POSTGRES_USER'],
    password=config['POSTGRES_PASSWORD']
    )
saver = Saver(db)

def get_page(page_number: int) -> bytes:
    if page_number == 1:
        html_page = 'solution/elsevier/JournalsScraper/sample_data/site_search.html'
    else:
        html_page = 'solution/elsevier/JournalsScraper/sample_data/last_page.html'
    with open(html_page, 'rb') as fp:
        return fp.read()


program.main(get_page, program.get_journals, saver.save)