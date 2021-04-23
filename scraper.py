import concurrent.futures
import json

import numpy as np
import psycopg2
import requests
# noinspection PyProtectedMember
from bs4 import BeautifulSoup, SoupStrainer
from lxml import html
from psycopg2 import Error
from tqdm import tqdm, trange

from apiCreds import key

global connection
try:
    connection = psycopg2.connect(user='PyCharm',
                                  password='123456',
                                  host="127.0.0.1",
                                  port=5432,
                                  database="Movie Rec")

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
    cursor.close()

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)


def scrape_ids_from_imdb(url):
    path = '//*[@id="main"]/div/div[1]/div[2]/a/@href'
    if not url:
        url = 'http://www.imdb.com/search/title/?year=2005-01-01,2021-12-31&view=simple&start=0'
        path = '//*[@id="main"]/div/div[1]/div[2]/a/@href'
    tree = html.fromstring(requests.get(url).content)
    list_of_ids = tree.xpath('//*[@id="main"]/div/div[3]/div/div/div[2]/div/div[1]/span/span[2]/a/@href')
    address = tree.xpath(path)
    if len(address) == 2:
        address = address[1]
    else:
        address = address[0]
    return list_of_ids, address


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
    actor_names = []
    director_names = []

    links = tree.xpath('//*[@id="titleDetails"]/div[1]/a/@href')
    keywords = tree.xpath('//*[@id="titleStoryLine"]/div[2]/a/span/text()')
    isMovie = 'True' if get_stuffs(json_stuffs, "@type") == 'Movie' else 'False'
    title = get_stuffs(json_stuffs, "name").replace("'", "\\'")
    imdb_id = get_stuffs(json_stuffs, "url")[7:-1]
    image_url = get_stuffs(json_stuffs, "image")
    genre = get_stuffs(json_stuffs, "genre")
    actors = get_stuffs(json_stuffs, "actor")
    directors = get_stuffs(json_stuffs, "director")
    creators = get_stuffs(json_stuffs, "creator")
    description = get_stuffs(json_stuffs, "description").replace("'", "\\'")
    date_published = get_stuffs(json_stuffs, "datePublished")
    total_ratings = get_stuffs(json_stuffs, "aggregateRating", "ratingCount")
    rating_value = get_stuffs(json_stuffs, "aggregateRating", "ratingValue")
    total_ratings = total_ratings if total_ratings else 0
    rating_value = rating_value if rating_value else 0
    tmdb_id = None
    tmdb_rating = 0
    tmdb_rating_count = 0

    genre = [genre] if isinstance(genre, str) else genre

    if isinstance(directors, dict):
        s = str(directors['name'])
        s = s.replace("'", "\\'")
        director_names.append(s)
    elif isinstance(directors, list):
        for director in directors:
            s = str(director['name'])
            s = s.replace("'", "\\'")
            director_names.append(s)

    if isinstance(creators, list):
        for creator in creators:
            try:
                director_names.append(creator['name'])
            except KeyError:
                continue

    for i in actors:
        s = str(i['name'])
        s = s.replace("'", "\\'")
        actor_names.append(s)

    id_type = 'movie' if isMovie == 'True' else 'tv'
    api_link = f'https://api.themoviedb.org/3/find/{imdb_id}?api_key={key}&language=en-US&external_source=imdb_id'
    api_output = json.loads(requests.get(api_link).content)
    try:
        tmdb_id = (api_output['tv_results'] + api_output['movie_results'])[0]['id']
    except IndexError:
        pass

    if tmdb_id:
        api_link = f'https://api.themoviedb.org/3/{id_type}/{imdb_id}?api_key={key}&language=en-US'
        api_output = json.loads(requests.get(api_link).content)

        try:
            tmdb_rating = api_output['vote_average']
            tmdb_rating_count = api_output['vote_count']
        except KeyError:
            tmdb_rating = 0
            tmdb_rating_count = 0

    genre = ",".join(genre)
    actor_names = ",".join(actor_names)
    director_names = ",".join(director_names)
    keywords = ",".join(keywords).replace("'", "\\'")
    links = ",".join(links)
    # columns = ['IMDB ID', 'Title', 'Is a Movie', 'Genre', 'Actors', 'Director', 'Description', 'Date Published',
    #            'Keywords', 'Number of Ratings', 'Rating', 'External links', 'TMDB ID', 'TMDB Rating',
    #            'TMDB Rating Count', "Image URL"]

    with connection.cursor() as cur:
        try:
            cur.execute(
                f"""INSERT INTO "Everything" ("IMDB ID", "TMDB Rating Count", "TMDB Rating", "Ratings", "Number of Ratings", "Image URL", "External Links", "Keywords", "Date Published", "Directors", "Actors", "Genre", "isMovie", "Title", "Plot", "TMDB ID") VALUES ('{imdb_id}', {tmdb_rating_count}, {tmdb_rating}, {rating_value}, {total_ratings}, '{image_url}', '{links}', E'{keywords}', '{date_published}', E'{director_names}', E'{actor_names}', '{genre}', {isMovie}, E'{title}', E'{description}', '{tmdb_id}') ON CONFLICT DO NOTHING ;""")
        except:
            pass
        connection.commit()


def main():
    entries_to_scrape = 20000
    no_of_pages = entries_to_scrape // 50
    base_url = "http://www.imdb.com"
    imdb_ids = []
    imdb_links = []
    addr = None

    for _ in trange(no_of_pages):
        # TODO mulltithread the first 200 pages
        scraped_ids, addr = scrape_ids_from_imdb(addr)
        addr = base_url + addr
        imdb_ids.append(scraped_ids)

    imdb_ids = np.array(imdb_ids).flatten()
    for ID in imdb_ids:
        imdb_links.append(base_url + ID)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        _ = list(tqdm(executor.map(scrape_imdb_page, imdb_links), total=entries_to_scrape))

    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


if __name__ == '__main__':
    main()
