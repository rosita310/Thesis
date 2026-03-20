import requests
import logging
import codecs
from bs4 import BeautifulSoup
import time
import datetime
from database import Postgress, Saver
import json
import configparser

logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

DBLP_API_DATABASE_SCHEMA = 'dblp_api'

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


def read_content(content) -> dict:
    result = content["result"]
    ret = {
        'document': [],
        'author': []
    }
    if "hit" not in result["hits"]:
        return ret
    for r in result["hits"]["hit"]:
        d = get_doc_info(r)
        ret['document'].append(d)
        ret['author'].extend(get_authors(r, d["@id"]))
    return ret


def get_doc_info(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                if a != 'authors':
                    flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def get_authors(y, doc_id):
    x = []
    if "authors" in y["info"]:
        a = y["info"]["authors"]
        z = a['author']
        if isinstance(z, dict):
            x.append(z)
        else:
            for ai in z:
                ai["$_doc_id"] = doc_id
                x.append(ai)
    return x


def execute_request(query) -> dict:
    #print(f"Executing query: {query}")
    dat = {}
    dat["document"] = []
    dat["author"] = []
    def req(i, query_in) -> dict:
        hits = 1000
        first = i * hits
        q = query_in.replace("/", "%2F")
        query = f"stream%3Astreams%2F{q}%3A editorship"
        # ":facetid:stream:streams\"/\"conf\"/\"cpm"
        # stream%3Astreams%2Fconf%2Fcpm%3A
        # print(f"Request for query: {query}, first: {first}")
        content = {}
        content["document"] = []
        content["author"] = []
        url = f"https://dblp.uni-trier.de/search/publ/api?q={query}&h={hits}&format=json&f={first}"
        response = requests.get(url)
        timestamp = str(datetime.datetime.now())
        if response.status_code != 200:
            pass
            # print(f"Got return code {response.status_code}: stopping")
        else:
            h = read_content(json.loads(response.content))
            for x in h["document"]:
                x["$_api_extract_dts"] = timestamp
                x["$_url"] = url
                x["$_query"] = query
                content["document"].append(x)
            content["author"].extend(h["author"])
        return content
    
    i = 0
    while(True):
        hits = req(i, query)
        if len(hits["document"]) == 0:
            # print(f"Returned 0 hits: stopping")
            break
        dat["document"].extend(hits["document"])
        dat["author"].extend(hits["author"])
        i = i+1
        time.sleep(1)
    
    c = len(dat["document"])
    print(f"Returning: {c} hits")
    return dat


def get_queries() -> list:
    logging.info("Fetch queries from database")
    query = f"SELECT dblp_key FROM core.conferences WHERE dblp_key IS NOT NULL"

    query_result = db.execute_query_result(query)
    result = [x['dblp_key'] for x in query_result]
    return result


def equal_list_of_dicts(l):
    keys = []
    ret = []
    for d in l:
        for k in d:
            if k not in keys:
                keys.append(k)
    for d in l:
        new_d = {}
        for k in sorted(keys):
            new_d[k] = d[k] if k in d else None
        ret.append(new_d)
    return ret


def main():
    print("Program started")
    queries = get_queries()
    total_queries = len(queries)
    i = 1
    for q in queries:
        #print(f"Query {i} / {total_queries}")
        dt = execute_request(q)
        #print("Writing to database")
        for x in dt:
            d = equal_list_of_dicts(dt[x])
            saver.save(DBLP_API_DATABASE_SCHEMA, x, d)
        i = i + 1
    print("Done")


if __name__ == '__main__':
    main()
