import requests
import logging
import os
import configparser

logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')

def read_config(path) -> configparser.SectionProxy:
    logging.info('Reading configuration')
    with open(path, 'r') as f:
        config_string = '[SECTION]\n' + f.read()
    config = configparser.ConfigParser()
    config.read_string(config_string)
    return config['SECTION']

config = read_config('../.env')

# Directory with HTML pages
output_dir = os.path.join(config["RAW_DATA"], config["ACM_EDITORIAL_BOARD_HTML_SUBDIR"])

# To be able to start from a certain file for debugging
start_num = 0

def main():
    """
    Reads input file.
    """
    with open('solution/acm/list_journals.txt') as f:
        i = 0
        for l in f.readlines():
            i = i + 1
            if i >= start_num:
                process_link(l, i)
            


def process_link(link: str, i: int):
    """
    Downloads HTML page of a link.
    """
    logging.debug(link)
    try:
        redirect = requests.get(link)
        logging.debug(redirect.url)
        editors_url = redirect.url + '/editorial-board'
        editorial_board_content = requests.get(editors_url)
        with open(f"{output_dir}/{i}.html", "w") as f:
            f.write(editorial_board_content.text)
    except Exception as e:
        logging.error(f"{i} failed")
        logging.exception(e)




if __name__ == '__main__':
    logging.debug('Program start')
    main()
    logging.debug('Program finished')
