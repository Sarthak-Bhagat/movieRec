import concurrent.futures
import json

import numpy as np
import pandas as pd
import requests
# noinspection PyProtectedMember
from bs4 import BeautifulSoup, SoupStrainer
from lxml import html
from tqdm import tqdm, trange

from apiCreds import key


def scrape_ids_from_imdb(url):
    if not url:
        url = 'http://www.imdb.com/search/title/?year=2005-01-01,2021-12-31&view=simple&start=0'
    tree = html.fromstring(requests.get(url).content)
    list_of_ids = tree.xpath('//*[@id="main"]/div/div[3]/div/div/div[2]/div/div[1]/span/span[2]/a/@href')
    address = tree.xpath('//*[@id="main"]/div/div[1]/div[2]/a/@href')
    return list_of_ids, address[0]


def get_stuffs(json_stuffs, field, field2=""):
    try:
        if field2 != "":
            return json_stuffs[field][field2]
        return json_stuffs[field]
    except KeyError:
        return ""


def scrape_imdb_page(url):
    page = requests.get(url)
    strainer = SoupStrainer('script', type="application/ld+json")
    soup = BeautifulSoup(page.content, "lxml", parse_only=strainer)
    script_api = str(soup.contents[1])[35:-9]
    json_stuffs = json.loads(script_api)
    tree = html.fromstring(page.content)
    columns = ['IMDB ID', 'Title', 'Type', 'Genre', 'Actors', 'Director', 'Description', 'Date Published', 'Keywords',
               'Number of Ratings', 'Rating', 'External links', 'TMDB ID', 'TMDB Rating', 'TMDB Rating Count',
               "Image URL"]
    actor_names = []
    director_names = []
    creator_names = []

    links = tree.xpath('//*[@id="titleDetails"]/div[1]/a/@href')
    keywords = tree.xpath('//*[@id="titleStoryLine"]/div[2]/a/span/text()')
    isMovie = True if get_stuffs(json_stuffs, "@type") == 'Movie' else False
    title = get_stuffs(json_stuffs, "name")
    imdb_id = get_stuffs(json_stuffs, "url")[7:-1]
    image_url = get_stuffs(json_stuffs, "image")
    genre = get_stuffs(json_stuffs, "genre")
    actors = get_stuffs(json_stuffs, "actor")
    directors = get_stuffs(json_stuffs, "director")
    creators = get_stuffs(json_stuffs, "creator")
    description = get_stuffs(json_stuffs, "description")
    date_published = get_stuffs(json_stuffs, "datePublished")
    total_ratings = get_stuffs(json_stuffs, "aggregateRating", "ratingCount")
    rating_value = get_stuffs(json_stuffs, "aggregateRating", "ratingValue")

    if isinstance(directors, dict):
        director_names.append(directors['name'])
    elif isinstance(directors, list):
        for director in directors:
            director_names.append(director['name'])

    if isinstance(creators, list):
        for creator in creators:
            try:
                director_names.append(creator['name'])
            except KeyError:
                continue

    for i in actors:
        actor_names.append(i['name'])

    id_type = 'movie' if isMovie else 'tv'
    api_link = f'https://api.themoviedb.org/3/find/{imdb_id}?api_key={key}&language=en-US&external_source=imdb_id'
    api_output = json.loads(requests.get(api_link).content)
    tmdb_id = (api_output['tv_results'] + api_output['movie_results'])[0]['id']
    api_link = f'https://api.themoviedb.org/3/{id_type}/{imdb_id}?api_key={key}&language=en-US'
    api_output = json.loads(requests.get(api_link).content)

    try:
        tmdb_rating = api_output['vote_average']
        tmdb_rating_count = api_output['vote_count']
    except KeyError:
        tmdb_rating = 0
        tmdb_rating_count = 0

    df = pd.DataFrame(
        [[imdb_id, title, isMovie, genre, actor_names, director_names, description, date_published, keywords,
          total_ratings, rating_value, links, tmdb_id, tmdb_rating, tmdb_rating_count, image_url]],
        columns=columns)
    return df


def main():
    columns = ['IMDB ID', 'Title', 'Type', 'Genre', 'Actors', 'Director', 'Description', 'Date Published', 'Keywords',
               'Number of Ratings', 'Rating', 'External links', 'TMDB ID', 'TMDB Rating', 'TMDB Rating Count',
               "Image URL"]
    entries_to_scrape = 100
    no_of_pages = entries_to_scrape // 50
    base_url = "http://www.imdb.com"
    imdb_ids = []
    imdb_links = []
    addr = None
    imdb_database = pd.DataFrame(columns=columns)

    for _ in trange(no_of_pages):
        # TODO mulltithread the first 200 pages
        scraped_ids, addr = scrape_ids_from_imdb(addr)
        addr = base_url + addr
        imdb_ids.append(scraped_ids)

    imdb_ids = np.array(imdb_ids).flatten()

    for ID in imdb_ids:
        imdb_links.append(base_url + ID)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(scrape_imdb_page, imdb_links), total=entries_to_scrape))
        for result in results:
            imdb_database = pd.concat([imdb_database, result], ignore_index=True)

    imdb_database.to_csv('uwu.csv', mode='w+', index=False)


if __name__ == '__main__':
    main()
