import logging
from solution.PCMemberAttack.Springer.lncs.scraper.helpers import clean_value

def get_affiliations(soup) -> list:
    logging.debug("get_affiliations")
    affiliations = soup.find_all('li', class_="affiliation")
    aff_list = []
    for aff in affiliations:
        a = {}
        a["id"] = aff["data-test"]
        for x in ["department", "name", "city", "country"]:
            elem = aff.find('span', class_=f"affiliation__{x}")
            if elem is not None:
                a[x] = clean_value(elem.text)
        aff_list.append(a)
    logging.debug(f"get_affiliations() returned {len(aff_list)} objects")
    return aff_list