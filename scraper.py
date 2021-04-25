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
skipped = 0
try:
    connection = psycopg2.connect(user="PyCharm",
                                  password="123456",
                                  database="Movie Rec")

    # Creates a cursor to perform database operations
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
    tree = html.fromstring(requests.get(url).content)
    list_of_ids = tree.xpath('//*[@id="main"]/div/div[3]/div/div/div[2]/div/div[1]/span/span[2]/a/@href')
    return list_of_ids


def get_stuffs(json_stuffs, field, field2=""):
    try:
        if field2 != "":
            return json_stuffs[field][field2]
        return json_stuffs[field]
    except KeyError:
        return ""


def scrape_imdb_page(url):
    global skipped
    page = requests.get(url)
    strainer = SoupStrainer('script', type="application/ld+json")
    soup = BeautifulSoup(page.content, "lxml", parse_only=strainer)
    script_api = str(soup.contents[1])[35:-9]
    json_stuffs = json.loads(script_api)
    tree = html.fromstring(page.content)
    actor_names = []
    director_names = []

    links = tree.xpath('//*[@id="titleDetails"]/div[1]/a/@href')
    links = [i.replace("'", "\\'") for i in links]
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
    date_published = '1970-01-01' if date_published == '' else date_published

    genre = [genre] if isinstance(genre, str) else genre

    if isinstance(directors, dict):
        try:
            s = str(directors['name'])
            s = s.replace("'", "\\'")
            director_names.append(s)
        except TypeError:
            pass
    elif isinstance(directors, list):
        for director in directors:
            try:
                s = str(director['name'])
                s = s.replace("'", "\\'")
                director_names.append(s)
            except TypeError:
                pass

    if isinstance(creators, list):
        for creator in creators:
            try:
                s = str(creator['name'])
                s = s.replace("'", "\\'")
                director_names.append(s)
            except KeyError:
                continue

    for i in actors:
        try:
            s = str(i['name'])
            s = s.replace("'", "\\'")
            actor_names.append(s)
        except TypeError:
            pass

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

    with connection.cursor() as cur:
        try:
            cmd = f"""INSERT INTO "Everything" ("IMDB ID", "TMDB Rating Count", "TMDB Rating", "Ratings", "Number of Ratings", "Image URL", "External Links", "Keywords", "Date Published", "Directors", "Actors", "Genre", "isMovie", "Title", "Plot", "TMDB ID") VALUES ('{imdb_id}', {tmdb_rating_count}, {tmdb_rating}, {rating_value}, {total_ratings}, '{image_url}', E'{links}', E'{keywords}', '{date_published}', E'{director_names}', E'{actor_names}', '{genre}', {isMovie}, E'{title}', E'{description}', '{tmdb_id}') ON CONFLICT DO NOTHING ;"""
            cur.execute(cmd)
            connection.commit()
        except psycopg2.errors.InFailedSqlTransaction:
            skipped += 1
            print(skipped)


def main():
    entries_to_scrape = 1000
    base_url = "http://www.imdb.com"
    imdb_ids = []
    ent = entries_to_scrape
    adr = [f'http://www.imdb.com/search/title/?year={year}-01-01,{year}-12-31&view=simple&start={i}' for i in
           range(0, ent, 50) for year in range(2000, 2021)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(scrape_ids_from_imdb, adr), total=len(adr)))
        for result in results:
            imdb_ids.append(result)

    imdb_ids = np.array(imdb_ids).flatten()
    imdb_links = [str(base_url+ID) for ID in imdb_ids]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        _ = list(tqdm(executor.map(scrape_imdb_page, imdb_links), total=len(adr)*50))

    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


if __name__ == '__main__':
    main()
