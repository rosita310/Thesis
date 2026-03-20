import logging
import configparser
import database
import saver
import pyodbc
import requests
import datetime
import os
import sys


def read_config(path) -> configparser.SectionProxy:
    logging.info('Reading configuration')
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

config = read_config('../.env')

output_dir = os.path.join(config["RAW_DATA"], config["LNCS_FRONT_MATTER_SUBDIR"])

server = 'localhost'
db_name = 'study'
driver = '{ODBC Driver 17 for SQL Server}'
conn_str = f'Driver={driver};Server={server};Database={db_name};Trusted_Connection=yes;'
schema_name = 'springer_lncs'

db = database.SqlServer(conn_str)
saver = saver.Saver(db)

def get_workload():
    logging.debug("get_workload")
    query = "SELECT [$_dblp_key], [front_matter_url] FROM [springer_lncs].[document_info]"
    if db.table_exists(schema_name, "download_process_info"):
        query = f"""
        SELECT [$_dblp_key], [front_matter_url] 
        FROM [springer_lncs].[document_info]
        WHERE [$_dblp_key] NOT IN 
        (
            SELECT [$_dblp_key] 
            FROM [{schema_name}].[download_process_info]
        )
        """
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        res = []
        while row:
            res.append({
                "$_dblp_key": str(row[0]),
                "front_matter_url": str(row[1])
                })
            row = cursor.fetchone()
        return res
    except:
        logging.error("error occured while getting workload from database")
    finally:
        cursor.close()


def clean_dblp(input) -> str:
    return input.replace("/", "_").strip()


def download_front_matter(workitem):
    logging.debug(f"Downloading workitem: {workitem}")
    try:
        download_url = "https://link.springer.com" + workitem["front_matter_url"]
        logging.debug(f"Download url {download_url}")
        workitem["download_dts"] = str(datetime.datetime.now())
        r = requests.get(download_url, allow_redirects=True)
        output_filename = os.path.join(output_dir, clean_dblp(workitem["$_dblp_key"]) + ".pdf")
        workitem["location"] = output_filename
        open(output_filename, 'wb').write(r.content)
        status = "SUCCEEDED"
    except: # catch *all* exceptions
        error = sys.exc_info()[0]
        workitem["error_message"] = error
        status = "FAILED"
    workitem["status"] = status
    logging.debug(f"Result : {workitem}")
    saver.save(schema_name, "download_process_info", [workitem])


def run():
    logging.info("Program started")
    workload = get_workload()
    i = 0
    total = len(workload)
    for workitem in workload:
        i += 1
        logging.debug(f"Processing {i} of {total}")
        download_front_matter(workitem)
    logging.info("Program ended")
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()