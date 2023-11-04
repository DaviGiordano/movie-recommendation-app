# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -

import requests
from dotenv import load_dotenv
import duckdb
import os

def init_duck_db_movies(path, data):
    conn = duckdb.connect(path, read_only=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS movies(
        genre_ids INT[],
        id INTEGER,
        original_language VARCHAR,
        overview VARCHAR,
        popularity DOUBLE,
        release_date TIMESTAMP,
        title VARCHAR,
        vote_average DOUBLE,
        vote_count INTEGER
        );
        """
    )

    for movie in data['results']:
        genre_ids_str = ','.join(map(str, movie['genre_ids']))  # Creates a string with the genre ids separated by comma
        conn.execute(f""" INSERT INTO movies VALUES(ARRAY[{genre_ids_str}],
            {movie["id"]},
            '{movie["original_language"]}',
            '{movie["overview"].replace("'", "''")}',
            {movie["popularity"]},
            '{movie["release_date"]}',
            '{movie["title"].replace("'", "''")}',
            {movie["vote_average"]},
            {movie["vote_count"]});
        """
        )
    conn.close()
    

def init_duck_db_genres(path, data):
    conn = duckdb.connect(path, read_only=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS genres(
            id INTEGER,
            name VARCHAR
        );
        """
    )
    for genre in data['genres']:
        conn.execute(f""" INSERT INTO genres VALUES(
            {genre['id']},
            '{genre["name"]}'
        );
    """)
    conn.close()

# Load API key from .env file
load_dotenv('.env')
api_key = os.getenv('API_KEY')

# Construct URL
url_genres = f"https://api.themoviedb.org/3/genre/movie/list?api_key={api_key}&with_original_language=en"

# +
number_of_pages = 100
# -


try:
    # Request genre data
    genre_res = requests.get(url_genres)
except requests.exceptions.RequestException as e:
    print('An error occured during the genre request:', e)

list_of_pages = []
try:
    # Request movie data
    for i in range(number_of_pages):
        url_movies = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}&language=en-US&page={i+1}&sort_by=popularity.desc"
        list_of_pages.append(requests.get(url_movies).json())
        print(f'>> Retrieving page {i}')
except requests.exceptions.RequestException as e:
    print('An error occured during the movie request:', e)

# Merging pages from movie dictionaries
movies_res = {'results':[]}

for page in list_of_pages:
    movies_res["results"].extend(page["results"])
genre_res = genre_res.json()

# Initialize connection with database
duckdb_file_path = 'movies_data.duckdb'

# Create duckdb tables and populate
init_duck_db_movies(duckdb_file_path, movies_res)
init_duck_db_genres(duckdb_file_path, genre_res)
