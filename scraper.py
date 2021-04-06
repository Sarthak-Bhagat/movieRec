import concurrent.futures
import json
import pandas as pd
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


@dispatch(dict, str)
def getStuffs(JSON, field):
    try:
        return JSON[field]
    except:
        return ""


@dispatch(dict, str, str)
def getStuffs(JSON, field, field2):
    try:
        return JSON[field][field2]
    except:
        return ""


def scrape_imdb_page(URL):
    page = requests.get(URL)
    strainer = SoupStrainer('script', type="application/ld+json")
    soup = BeautifulSoup(page.content, "lxml", parse_only=strainer)
    scriptAPI = str(soup.contents[1])[35:-9]
    jsonStuffs = json.loads(scriptAPI)
    columns = ['IMDB ID', 'Type', 'Genre', 'Actors', 'Director', 'Description', 'Date Published', 'Keywords', 'Number of Ratings', 'Rating', 'Image Url']

    movie_or_show = getStuffs(jsonStuffs, "@type")
    imdb_id = getStuffs(jsonStuffs, "url")[7:-1]
    image_url = getStuffs(jsonStuffs, "image")
    genre = getStuffs(jsonStuffs, "genre")
    actors = getStuffs(jsonStuffs, "actor")
    director = getStuffs(jsonStuffs, "director")
    creator = getStuffs(jsonStuffs, "creator")
    description = getStuffs(jsonStuffs, "description")
    datePublished = getStuffs(jsonStuffs, "datePublished")
    keywords = getStuffs(jsonStuffs, "keywords")
    totalRatings = getStuffs(jsonStuffs, "aggregateRating", "ratingCount")
    ratingValue = getStuffs(jsonStuffs, "aggregateRating", "ratingValue")
    df = pd.DataFrame([imdb_id, movie_or_show, genre, actors, director, description, datePublished, keywords, totalRatings, ratingValue, image_url], columns=columns)
    print(df)


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
