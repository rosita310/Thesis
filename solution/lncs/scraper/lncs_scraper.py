import codecs
from bs4 import BeautifulSoup
import logging
import datetime
import database
import requests
import time
import sys
sys.path.append('./solution/python_packages/database')
from database import Postgress, Saver 
import configparser
from helpers import clean_value
from person_affiliation import get_affiliations
import pyodbc
import os
import paper

logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')

logging.info(f"Working directory: {os.getcwd()}")

# Database schema the data should be stored
schema_name = 'springer_lncs'

def read_config(path) -> configparser.SectionProxy:
    logging.info('Reading configuration')
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']


config = read_config('../.env')
db = Postgress(
    server=config['POSTGRES_SERVER'], 
    database=config['POSTGRES_DB'],
    user=config['POSTGRES_USER'],
    password=config['POSTGRES_PASSWORD']
    )
saver = Saver(db)


def get_bibliographic_information(soup):
    bi = soup.find('div', id="bibliographic-info")
    items = bi.find_all('li', class_="bibliographic-information__item")
    return items


def get_document_info(soup) -> dict:
    document_info = {}
    for i in get_bibliographic_information(soup):
        title_elem = i.find('span', class_="bibliographic-information__title")
        if title_elem is None:
            continue
        value_elem = i.find('span', class_="bibliographic-information__value")
        title = clean_value(title_elem.text)
        if title == 'Editors' or title == 'Topics':
            continue
        value = clean_value(value_elem.text)
        document_info[title.replace(' ', '_').lower()] = value
    front_matter_item = soup \
        .find('div', class_="book-toc-container", id="booktoc") \
        .find('ol', class_="content-type-list", recursive=False) \
        .find('li', class_="front-matter-item", recursive=False) \
        .find('a', attrs={"aria-label": 'Download PDF - Front Matter'})
    document_info["front_matter_url"] = front_matter_item["href"]
    return document_info


def get_editors_from_document_info(soup) -> list:
    logging.debug("get_editors_from_document_info")
    ret = []
    for i in get_bibliographic_information(soup):
        title_elem = i.find('span', class_="bibliographic-information__title")
        if title_elem is not None and clean_value(title_elem.text) == 'Editors':
            value_elem = i.find('span', class_="bibliographic-information__value")
            id = 0
            for editor_name in value_elem.contents:
                x = str(editor_name)
                if x.replace(' ', '') == '<br/>':
                    continue
                editor_ret = {}
                id = id + 1
                editor_ret["editor_id"] = str(id)
                editor_ret["name"] = clean_value(str(editor_name))
                ret.append(editor_ret)
    logging.debug(f"get_editors_from_document_info() returned {len(ret)} objects")
    return ret


def get_chapters_links(soup) -> list:
    logging.info('getting chapters')
    ret = []
    chapters = soup.find_all('li', class_=["chapter-item"])
    for c in chapters:
        anchor = c.find('a', class_='u-interface-link')
        link = anchor['href']
        ret.append(link)
    return ret


def get_editors(soup) -> list:
    """
    Gets editors from a book (proceeding) page.

    args:
        soup: BeautifulSoup object of the page.
    """
    logging.debug("get_editors")
    ed_and_af = soup.find('div', id="editorsandaffiliations")
    if ed_and_af is None:
        logging.debug("no editors and affiliations in document")
        return []
    lis = ed_and_af.find_all('li')
    ret = []
    editor_id = 0
    for l in lis:
        if "itemprop" in l.attrs and l["itemprop"] == 'editor':
            editor_id = editor_id + 1
            editor = {}
            editor["editor_id"] = str(editor_id)
            name_elem = l.find('span', class_="authors-affiliations__name")
            editor["name"] = clean_value(name_elem.text)
            orc_elem = l.find('span', class_="author-information__orcid")
            if orc_elem is not None:
                editor["orc"] = orc_elem.find('a')["href"]
            aff_list = l.find('ul', class_="authors-affiliations__indexes")
            if aff_list is not None:
                editor["affiliation_ids"] = [clean_value(aff["data-affiliation"]) for aff in aff_list.find_all('li')]
            ret.append(editor)
    logging.debug(f"get_editors() returned {len(ret)} objects")
    return ret


def read_content(content, dblp_key):
    """
    Reads a book (proceeding) page.
    """
    soup = BeautifulSoup(content, 'html.parser')
    content = {}
    document_info = get_document_info(soup)
    content["book"] = document_info
    editors = get_editors(soup)
    if len(editors) == 0:
        logging.warn(f"Unable to find editors, falling back to get editors from document info")
        editors = get_editors_from_document_info(soup)
    
    affiliations = get_affiliations(soup)
    editor_affiliation = add_editor_to_affiliation(editors, affiliations)
    for e in editors:
        if 'affiliation_ids' in e:
            del e['affiliation_ids']
    content["editor"] = editors
    content["editor_affiliation"] = editor_affiliation

    paper_lists = ['chapter', 'author', 'author_affiliation']
    for pl in paper_lists:
        content[pl] = []
    
    chapter_links = get_chapters_links(soup)
    for chapter_link in chapter_links:
        raw_paper_content = get_content_from_url(f'https://link.springer.com{chapter_link}')
        paper_content = paper.process_paper_page(content=raw_paper_content)
        for pl in paper_lists:
            if isinstance(paper_content[pl], dict):
                paper_content[pl]["$_paper_url"] = chapter_link
                content[pl].append(paper_content[pl])
            if isinstance(paper_content[pl], list):
                for d in paper_content[pl]:
                    d["$_paper_url"] = chapter_link
                    content[pl].append(d)

    return content


def add_editor_to_affiliation(editors, affiliations) -> list:
    logging.debug("add_editor_to_affiliation")
    ret = []
    for editor in editors:
        if "affiliation_ids" not in editor:
            continue
        for editor_affiliation_id in editor["affiliation_ids"]:
            for affiliation in affiliations:
                if affiliation["id"] == editor_affiliation_id:
                    aa = {}
                    aa["editor_id"] = editor["editor_id"]
                    aa["affiliation_id"] = editor_affiliation_id
                    for k, v in affiliation.items():
                        if k == 'id':
                            continue
                        aa[k] = v
                    # print(aa)
                    ret.append(aa)
    logging.debug(f"add_editor_to_affiliation() returned {len(ret)} objects")
    return ret


def process_content_entry(timestamp, url, dblp_key, content):
    """
    Adds static values to the datasets.
    Flushes the data to the database.
    """
    info = read_content(content, dblp_key)
    for k in info:
        if isinstance(info[k], dict):
            l = []
            info[k]["$_extract_dts"] = timestamp
            info[k]["$_book_url"] = url
            info[k]["$_dblp_key"] = dblp_key
            l.append(info[k])
            saver.save(schema_name, k, l)
        if isinstance(info[k], list):
            for d in info[k]:
                d["$_extract_dts"] = timestamp
                d["$_book_url"] = url
                d["$_dblp_key"] = dblp_key
            saver.save(schema_name, k, info[k])
    


def get_content_from_url(url) -> str:
    logging.debug("get_content_from_url()")
    response = requests.get(url)
    return response.text


def get_workload():
    """
    Returns the items which have not been processed previously.
    """
    logging.debug("get_workload")
    query = "SELECT dblp_key, url FROM dblp_api.lncs"
    if db.table_exists(schema_name, "book"):
        query = f"""
        SELECT dblp_key, url
        FROM dblp_api.lncs
        WHERE dblp_key NOT IN 
        (
            SELECT "$_dblp_key"
            FROM {schema_name}.book
        )
        """
    result = db.execute_query_result(query)
    return result


def main_process():
    workload = get_workload()
    rec_to_do = len(workload)
    i = 0
    for workitem in workload:
        url = workitem["url"]
        dblp_key = workitem["dblp_key"]
        try:
            logging.info(f"Processing {i+1} of {rec_to_do}")
            if i % 10 == 0:
                time.sleep(5)
            timestamp = str(datetime.datetime.now())
            content = get_content_from_url(url)
            process_content_entry(timestamp, url, dblp_key, content)
        except:
            logging.error(f"Unable to process: {dblp_key}")
        i += 1



def test():
    logging.basicConfig(level=logging.DEBUG)

    logging.debug("test")
    files = [
        'solution/lncs/scraper/examples/single_book.html',
        'solution/lncs/scraper/examples/test2.html',
        'solution/lncs/scraper/examples/oldest.html'
    ]
    i = 1
    for x in files:
        f=codecs.open(x, 'r', 'utf-8')
        content = f.read()
        process_content_entry(str(datetime.datetime.now()), f'www.testurl_{i}.com', f'test_dblp_key_{i}', content)
        i += 1


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # test()
    logging.info(f"Program started")
    main_process()
    logging.info(f"Program finished")
