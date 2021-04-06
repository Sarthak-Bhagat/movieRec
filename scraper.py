import concurrent.futures
import json
from multipledispatch import dispatch
import numpy as np
import requests
import tqdm
from bs4 import BeautifulSoup, SoupStrainer


def scrape_ids_from_imdb(URL):
    page = requests.get(URL)
    strainer = SoupStrainer('a', href=True)
    soup = BeautifulSoup(page.content, "lxml", parse_only=strainer)
    list_of_ids = []
    for a in soup.find_all("a", href=lambda href: href.startswith("/title")):
        list_of_ids.append(a['href'][7:-1])
    list_of_ids = list(dict.fromkeys(list_of_ids))
    return(list_of_ids)


def scrape_imdb_page(URL):
    page = requests.get(URL)
    strainer = SoupStrainer('script', type="application/ld+json")
    soup = BeautifulSoup(page.content, "lxml", parse_only=strainer)
    scriptAPI = str(soup.contents[1])[35:-9]
    jsonStuffs = json.loads(scriptAPI)
    print(jsonStuffs)


def main():
    entries_to_scrape = 50
    no_of_pages = entries_to_scrape//50
    num = 0
    baseURL = "http://www.imdb.com/"
    imdb_ids = []
    imdb_links = []

    for i in tqdm.trange(no_of_pages):
        num = i*50
        addr = "search/title/?year=2005-01-01,2021-12-31&view=simple&start="
        imdb_ids.append(scrape_ids_from_imdb(baseURL+addr+str(num)))

    imdb_ids = np.array(imdb_ids).flatten()

    for ID in imdb_ids:
        imdb_links.append(f"{baseURL}title/{ID}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        i = 0
        results = executor.map(scrape_imdb_page, imdb_links)
        for result in results:
            pass

if __name__ == '__main__':
    main()
