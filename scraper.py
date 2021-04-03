import json
import csv
import requests
import time
import tqdm
from bs4 import BeautifulSoup, SoupStrainer
import concurrent.futures


def scrape(URL):
    page = requests.get(URL)
    strainer = SoupStrainer('a', href=True)
    soup = BeautifulSoup(page.content, "lxml", parse_only=strainer)
    return(soup)


def get_links_from_soup(soup):
    L = []
    for a in soup.find_all("a", href=lambda href: href.startswith("/title")):
        L.append(a['href'])
    L = list(dict.fromkeys(L))
    for i in L:
        print(i[7:-1])


def main():
    entries_to_scrape = 50
    no_of_pages = entries_to_scrape//50
    num = 0
    baseURL = "http://www.imdb.com/search/title"
    scraped_pages = []
    start = time.perf_counter()

    for i in tqdm.trange(no_of_pages):
        num = i*50
        addr = f"/?year=2005-01-01,2021-12-31&view=simple&start={num}"
        scraped_pages.append(scrape(baseURL+addr))

    for i in scraped_pages:
        get_links_from_soup(i)

    finish = time.perf_counter()
    print(finish-start)


if __name__ == '__main__':
    main()
