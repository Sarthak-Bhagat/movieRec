import concurrent.futures
import json
import pandas as pd
import numpy as np
import requests
from lxml import html
from tqdm import tqdm,trange
from bs4 import BeautifulSoup, SoupStrainer


def scrape_ids_from_imdb(URL):
    page = requests.get(URL)
    tree = html.fromstring(page.content)
    list_of_ids= tree.xpath('//*[@id="main"]/div/div[3]/div/div/div[2]/div/div[1]/span/span[2]/a/@href')
    return(list_of_ids)


def getStuffs(JSON, field, field2 = ""):
    try:
        if field2 != "":
            return JSON[field][field2]
        return JSON[field]
    except:
        return ""


def scrape_imdb_page(URL):
    page = requests.get(URL)
    strainer = SoupStrainer('script', type="application/ld+json")
    soup = BeautifulSoup(page.content, "lxml", parse_only=strainer)
    scriptAPI = str(soup.contents[1])[35:-9]
    jsonStuffs = json.loads(scriptAPI)
    columns = ['IMDB ID', 'Type', 'Genre', 'Actors', 'Director', 'Description', 'Date Published', 'Keywords', 'Number of Ratings', 'Rating', 'Image Url', 'External links']
    tree = html.fromstring(page.content)
    links = tree.xpath('//*[@id="titleDetails"]/div[1]/a/@href')

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
    df = pd.DataFrame([[imdb_id, movie_or_show, genre, actors, director, description, datePublished, keywords, totalRatings, ratingValue, image_url, links]], columns=columns)
    return df


def main():
    columns = ['IMDB ID', 'Type', 'Genre', 'Actors', 'Director', 'Description', 'Date Published', 'Keywords', 'Number of Ratings', 'Rating', 'Image Url', 'External links']
    entries_to_scrape = 100 
    no_of_pages = entries_to_scrape//50
    num = 0
    baseURL = "http://www.imdb.com"
    imdb_ids = []
    imdb_links = []
    df = pd.DataFrame(columns=columns)

    for i in trange(no_of_pages):
        num = i*50
        addr = "/search/title/?year=2005-01-01,2021-12-31&view=simple&start="
        imdb_ids.append(scrape_ids_from_imdb(baseURL+addr+str(num)))

    imdb_ids = np.array(imdb_ids).flatten()

    for ID in imdb_ids:
        imdb_links.append(f"{baseURL}{ID}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(scrape_imdb_page, imdb_links), total=entries_to_scrape))
        for result in results:
            df = pd.concat([df, result], ignore_index=True)
    df.to_csv('uwu.csv',index=False)


if __name__ == '__main__':
    main()
