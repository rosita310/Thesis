import logging
import codecs
from bs4 import BeautifulSoup
from solution.PCMemberAttack.Springer.lncs.scraper.helpers import clean_value
from solution.PCMemberAttack.Springer.lncs.scraper.person_affiliation import get_affiliations


def process_paper_page(content: str) -> dict:
    logging.debug("process_paper_page()")
    info = {}
    soup = BeautifulSoup(content, 'html.parser')
    info['chapter'] = get_chapter_info(content)
    people = get_authors(soup)    
    affiliations = get_affiliations(soup)
    person_affiliation = add_person_to_affiliation(people, affiliations)
    for e in people:
        if 'affiliation_ids' in e:
            del e['affiliation_ids']
    info["author"] = people
    info["author_affiliation"] = person_affiliation
    return info


def get_bibliographic_information(soup):
    bi = soup.find('ul', class_="bibliographic-information__list")
    items = bi.find_all('li', class_="bibliographic-information__item")
    return items


def get_chapter_info(content:str):
    soup = BeautifulSoup(content, 'html.parser')
    chapter = {}

    for i in get_bibliographic_information(soup):
        title_elem = i.find('span', class_="bibliographic-information__title")
        if title_elem is None:
            continue
        value_elem = i.find('span', class_="bibliographic-information__value")
        title = clean_value(title_elem.text).replace(' ', '_').lower()
        value = clean_value(value_elem.text)
        chapter[title] = value

    title_element = soup.find('h1', class_='ChapterTitle')
    chapter['title'] = title_element.text
    return chapter


def add_person_to_affiliation(people, affiliations) -> list:
    logging.debug("add_person_to_affiliation")
    ret = []
    for person in people:
        if "affiliation_ids" not in person:
            continue
        for person_affiliation_id in person["affiliation_ids"]:
            for affiliation in affiliations:
                if affiliation["id"] == person_affiliation_id:
                    aa = {}
                    aa["doc_person_id"] = person["doc_person_id"]
                    aa["affiliation_id"] = person_affiliation_id
                    for k, v in affiliation.items():
                        if k == 'id':
                            continue
                        aa[k] = v
                    ret.append(aa)
    logging.debug(f"add_person_to_affiliation() returned {len(ret)} objects")
    return ret


def get_authors(soup) -> list:
    logging.debug("get_authors")
    ed_and_af = soup.find('section', id="authorsandaffiliations")
    if ed_and_af is None:
        logging.debug("no authors and affiliations in document")
        return []
    lis = ed_and_af.find_all('li')
    ret = []
    doc_person_id = 0
    for l in lis:
        if "itemprop" in l.attrs and l["itemprop"] == 'author':
            doc_person_id = doc_person_id + 1
            author = {}
            author["doc_person_id"] = str(doc_person_id)
            name_elem = l.find('span', class_="authors-affiliations__name")
            author["name"] = clean_value(name_elem.text)
            orc_elem = l.find('span', class_="author-information__orcid")
            if orc_elem is not None:
                author["orc"] = orc_elem.find('a')["href"]
            aff_list = l.find('ul', class_="authors-affiliations__indexes")
            if aff_list is not None:
                author["affiliation_ids"] = [clean_value(aff["data-affiliation"]) for aff in aff_list.find_all('li')]
            author_information = l.find('span', class_='author-information')
            if author_information is not None:
                spans = author_information.find_all('span')
                if spans is not None:
                    for s in spans:
                        children = s.find_all()
                        for c in children:
                            if "itemprop" in c.attrs:
                                elem = c["itemprop"]
                                value = None
                                if "title" in c.attrs:
                                    value = c["title"]
                                elif "href" in c.attrs:
                                    value = c["href"]
                                else:
                                    value = clean_value(c.text)
                                author[elem] = value
            ret.append(author)
    logging.debug(f"get_authors() returned {len(ret)} objects")
    return ret


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    logging.debug("test")
    files = [
        'solution/lncs/scraper/examples/paper_with_email.html'
    ]
    i = 1
    for x in files:
        f=codecs.open(x, 'r', 'utf-8')
        content = f.read()
        process_paper_page(content)