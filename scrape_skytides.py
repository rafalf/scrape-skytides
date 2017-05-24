#!/usr/bin/env python


# win: pip install lxml==3.6.0  (other pip install lxml)
# pip install requests
# pip install beautifulsoup4

import requests
from requests import ConnectionError
from bs4 import BeautifulSoup
import os
import sys
import getopt
import logging
import time
import csv
import re

scrape_url = 'http://skytides.com/research'
scrape_root = 'http://skytides.com'
logger = logging.getLogger(os.path.basename(__file__))

REGEX = '\([A-Z]+\)'
scrape_date = time.strftime('%Y-%m-%d', time.localtime())
scrape_time_mins = time.strftime('%H%M', time.localtime())


def scrape(fld, from_date, to_date):

    total_counter = 0

    logger.info('Scraping: {} for all articles'.format(scrape_url))
    r = requests.get(scrape_url)

    # create download folder
    if not fld:
        downloads_folder = os.path.join(os.path.dirname(__file__), 'download')
    else:
        downloads_folder = os.path.join(os.path.dirname(__file__), fld)
    if not os.path.isdir(downloads_folder):
        os.mkdir(downloads_folder)

    # find table
    soup = BeautifulSoup(r.text, 'lxml')
    table = soup.find(class_="views-table")

    # find table rows
    metadatas = []

    for row_ in table.find_all('tr'):
        metadata = []

        link_ = row_.find('a')

        # scrape date
        metadata.append(scrape_date)

        # title
        title_ = link_.text
        metadata.append(title_.strip().encode("utf-8"))

        #href
        href_ = link_.get('href')
        metadata.append(href_)

        # date
        td_ = row_.find_all('td')
        date_ = td_[0].text
        metadata.append(date_.strip())

        logger.info('article: {}\nDate: {}, Url: {}'.format(title_.strip().encode("utf-8"), date_.strip(), href_))

        metadatas.append(metadata)

    for each_article in metadatas:

        metadata_ = list(each_article)

        post_date = time.strptime(each_article[3], '%Y-%m-%d')
        post_date_secs = time.mktime(post_date)
        logger.info('Date in secs: %s' % post_date_secs)

        if from_date < post_date_secs < to_date:
            logger.info('Between start and end date -> Process')

            # folder
            split = each_article[3].split('-')
            year_numeric = split[0]
            month_numeric = split[1]
            day_numeric = split[2]
            folder_struc = os.path.join(downloads_folder, year_numeric, month_numeric, day_numeric,
                                        scrape_time_mins)
            if not os.path.isdir(folder_struc):
                os.makedirs(folder_struc)
                logger.info('Folders created: %s' % folder_struc)
            else:
                logger.info('Folders already exists: %s' % folder_struc)

            # individual articles
            logger.info('Scraping url: {}'.format(each_article[2]))
            logger.info('Title: {}'.format(each_article[1]))

            # construct a file name
            # ignore non ascii chars
            # strip all non alphanumeric

            file_core = each_article[1].decode('utf-8').encode('ascii', errors='ignore')
            file_core = file_core.replace(' - ', " ")

            split_title = file_core.lower().split(' ')
            file_core = "-".join(split_title)

            regex = re.compile(('[^a-zA-Z-]'))
            file_core = regex.sub('', file_core)
            file_core = file_core.replace('--', '-')
            if file_core.endswith('-'):
                file_core = file_core[:-1]

            # trim file
            while True:
                if file_core.count('-') > 10:
                    file_core = file_core[:file_core.rfind("-")]
                    logger.info('Trim file to: {}'.format(file_core))
                else:
                    logger.info('File core: {}'.format(file_core))
                    break

            for _ in range(3):

                try:
                    # get
                    request = requests.get(scrape_root + each_article[2], timeout=30, stream=True)
                    file_ = os.path.join(folder_struc, file_core + '.htm')
                    with open(file_, 'wb') as fh:
                        for chunk in request.iter_content(chunk_size=1024):
                            fh.write(chunk)
                    logger.info('Downloaded as: {}'.format(file_))

                    request = requests.get(scrape_root + each_article[2], timeout=30, stream=True)
                    soup = BeautifulSoup(request.text, 'lxml')

                    # kill all script and style elements
                    for script in soup(["script", "style"]):
                        script.extract()  # rip it out

                    # get html page text
                    html_text = soup.get_text()

                    # look up Tickers
                    matches = re.findall(REGEX, html_text, re.DOTALL)
                    logger.info('Tickers: ')
                    logger.info(matches)

                    if len(matches) > 0:
                        tickers = " ".join(matches)
                        metadata_.append(tickers)
                    else:
                        metadata_.append('None')

                    break
                except ConnectionError:
                    logger.info('ConnectionError --> retry up to 3 times')
            else:
                logger.error('ERROR: Failed to download')

            total_counter += 1

            # write metadata
            row = ['Processed Time', 'Report title', 'Report link', 'Publish Date', 'Tickers']
            _write_row(row, os.path.join(folder_struc, file_core + '.metadata.csv'))
            _write_row(metadata_, os.path.join(folder_struc, file_core + '.metadata.csv'))

        else:
            logger.info('Not between start and end date -> Skip')
            logger.info('! {} < {} < {}'.format(from_date, post_date_secs, to_date))

    logger.info('Total articles saved: {}'.format(total_counter))


def _write_row(row, full_path):
    with open(full_path, 'ab') as hlr:
        wrt = csv.writer(hlr, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        wrt.writerow(row)
        logger.debug('Added to %s file: %s' % (full_path, row))


if __name__ == '__main__':
    download_folder = None
    verbose = None
    from_date = '01/01/2000'
    to_date = '01/01/2100'

    log_file = os.path.join(os.path.dirname(__file__), 'logs',
                                time.strftime('%d%m%y', time.localtime()) + "_scraper.log")
    file_hndlr = logging.FileHandler(log_file)
    logger.addHandler(file_hndlr)
    console = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(console)
    ch = logging.Formatter('[%(levelname)s] %(message)s')
    console.setFormatter(ch)
    file_hndlr.setFormatter(ch)

    argv = sys.argv[1:]
    opts, args = getopt.getopt(argv, "o:vf:t", ["output=", "verbose", "from=", "to="])
    for opt, arg in opts:
        if opt in ("-o", "--output"):
            download_folder = arg
        elif opt in ("-f", "--from"):
            from_date = arg
        elif opt in ("-t", "--to"):
            to_date = arg
        elif opt in ("-v", "--verbose"):
            verbose = True

    str_time = time.strptime(from_date, '%m/%d/%Y')
    from_secs = time.mktime(str_time)

    str_time = time.strptime(to_date, '%m/%d/%Y')
    to_secs = time.mktime(str_time)

    if verbose:
        logger.setLevel(logging.getLevelName('DEBUG'))
    else:
        logger.setLevel(logging.getLevelName('INFO'))

    logger.info('CLI args: {}'.format(opts))
    logger.info('from: {}'.format(from_date))
    logger.info('to: {}'.format(to_date))
    logger.debug('from_in_secs: {}'.format(from_secs))
    logger.debug('to_in_secs: {}'.format(to_secs))

    scrape(download_folder, from_secs, to_secs)