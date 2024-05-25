import os
import urllib.parse
import pandas as pd
import numpy as np
import requests
import psycopg2
import time
from datetime import datetime

conn = psycopg2.connect(
  database='movies',
  host='localhost',
  user='user',
  password='p@ss',
  port='5432'
)

cursor = conn.cursor()

df = pd.read_excel('reports/source_report.xlsx')

titles = df['show_title'].to_list()

headers = {
	"accept": "application/json",
	"Authorization": os.getenv('TMDB_API_KEY')
}

def extract_movie_details(tmdb_id):
	movie_details = get_movie_details(tmdb_id)

	budget = movie_details['budget']
	tmdb_title = movie_details['title']
	revenue = movie_details['revenue']
	imdb_id = movie_details['imdb_id']

	movie_release_dates = get_movie_release_dates(tmdb_id)

	release_date_info = map_release_dates(movie_release_dates['results'])

	tmdb_title = tmdb_title.replace("'", "''")

	return {
		'imdb_id': imdb_id,
		'title': tmdb_title,
		'budget': budget,
		'release_date_streaming': release_date_info['streaming_release_date'],
		'country_release_date_streaming': release_date_info['country_streaming_release_date'],
		'all_countries_streaming': release_date_info['all_countries_streaming'],
		'release_date_theater': release_date_info['theater_release_date'],
		'country_release_date_theater': release_date_info['country_theater_release_date'],
		'all_countries_theater': release_date_info['all_countries_theater'],
		'revenue': revenue
	}
	
def get_movie_details(tmdb_id):
	movie_details_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?language=en-US"
	movie_details = requests.get(movie_details_url, headers=headers)
	details_status = movie_details.status_code
	while details_status != 200:
		time.sleep(50/1000)
		movie_details = requests.get(movie_details_url, headers=headers)
		details_status = movie_details.status_code
	
	return movie_details.json()

def get_movie_release_dates(tmdb_id):
	movie_release_dates_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/release_dates"
	movie_release_dates = requests.get(movie_release_dates_url, headers=headers)
	release_dates_status = movie_release_dates.status_code
	while release_dates_status != 200:
		time.sleep(50/1000)
		movie_release_dates = requests.get(movie_release_dates_url, headers=headers)
		release_dates_status = movie_release_dates.status_code
	
	return movie_release_dates.json()

def search_movie(title):
	parsed_query_filter = urllib.parse.quote(title)

	search_list_url = f"https://api.themoviedb.org/3/search/movie?query={parsed_query_filter}&include_adult=false&language=en-US&page=1"
	search_list = requests.get(search_list_url, headers=headers)
	search_status = search_list.status_code
	while search_status != 200:
		search_list = requests.get(search_list_url, headers=headers)
		search_status = search_list.status_code
	
	return search_list.json()

def map_release_dates(results):
	streaming_release_date, country_streaming_release_date = None, None
	theater_release_date, country_theater_release_date = None, None
	all_countries_streaming, all_countries_theater = set(), set()

	for result in results:
		for release in result['release_dates']:
			if release['type'] == 3:
				all_countries_theater.add(result['iso_3166_1'])
			
			if release['type'] == 4 and release['note'] == 'Netflix':
				all_countries_streaming.add(result['iso_3166_1'])


	for result in results:
		if result['iso_3166_1'] == 'US':
			for release in result['release_dates']:
				if release['type'] == 3:
					theater_release_date = release['release_date']
					country_theater_release_date = 'US'

				if release['type'] == 4 and release['note'] == 'Netflix':
					streaming_release_date = release['release_date']
					country_streaming_release_date = 'US'
		
		if result['iso_3166_1'] == 'CA':
			for release in result['release_dates']:
				if release['type'] == 3 and country_theater_release_date is not 'US':
						theater_release_date = release['release_date']
						country_theater_release_date = 'CA'

				if release['type'] == 4 and release['note'] == 'Netflix' and country_streaming_release_date is not 'US':
						streaming_release_date = release['release_date']
						country_streaming_release_date = 'CA'
		
		if streaming_release_date is None:
			for release in result['release_dates']:				
				if release['type'] == 4 and release['note'] == 'Netflix':
					streaming_release_date = release['release_date']
					country_streaming_release_date = result['iso_3166_1']
		
		if theater_release_date is None:
			for release in result['release_dates']:
				if release['type'] == 3:
						theater_release_date = release['release_date']
						country_theater_release_date = result['iso_3166_1']
		
	return {
		'streaming_release_date': streaming_release_date,
		'country_streaming_release_date': country_streaming_release_date,
		'all_countries_streaming': all_countries_streaming,
		'theater_release_date': theater_release_date,
		'country_theater_release_date': country_theater_release_date,
		'all_countries_theater': all_countries_theater
	}

def handle_field_to_db(field):
	if field is None:
		field = 'NULL'
	else:
		field = "'" + str(field) + "'"
	
	return field

def save(tmdb_id, data, report_title):
	tmdb_title = handle_field_to_db(data['title'].replace("'", "''"))
	report_title = handle_field_to_db(report_title.replace("'", "''"))
	budget = handle_field_to_db(data['budget'])
	release_date_streaming = handle_field_to_db(data['release_date_streaming'])
	country_release_date_streaming = handle_field_to_db(data['country_release_date_streaming'])
	all_countries_streaming = handle_field_to_db(','.join(str(x) for x in data['all_countries_streaming']))
	release_date_theater = handle_field_to_db(data['release_date_theater'])
	country_release_date_theater = handle_field_to_db(data['country_release_date_theater'])
	all_countries_theater = handle_field_to_db(','.join(str(x) for x in data['all_countries_theater']))
	revenue = handle_field_to_db(data['revenue'])
	imdb_id = handle_field_to_db(data['imdb_id'])

	query = f"""
		INSERT INTO tmdb_movie
			(
				tmdb_id,
				report_title,
				tmdb_title,
				budget,
				release_date_streaming,
				country_release_date_streaming,
				all_countries_streaming,
				release_date_theater,
				country_release_date_theater,
				all_countries_theater,
				revenue
			)
		VALUES
			(
				{tmdb_id},
				{report_title},
				{tmdb_title},
				{budget},
				{release_date_streaming},
				{country_release_date_streaming},
				{all_countries_streaming},
				{release_date_theater},
				{country_release_date_theater},
				{all_countries_theater},
				{revenue}				
			);
	"""

	try:
		cursor.execute(query)
		conn.commit()

		print('Inserted ' + data['title'] + '!!!')
	except Exception as e:
		conn.rollback()

		print('Could not insert movie ' + data['title'] + ' with id ' + str(tmdb_id) + ': ' + str(e))

	query = f"INSERT INTO imdb_movie (tmdb_id, imdb_id) VALUES ({tmdb_id}, {imdb_id});"
	try:
		cursor.execute(query)
		conn.commit()

		print('Inserted IMDB ' + str(data["imdb_id"]) + '!!!')
	except Exception as e:
		conn.rollback()

		print('Could not insert imdb movie ' + str(data["imdb_id"]) + ': ' + str(e))

query = 'SELECT report_title FROM tmdb_movie'

cursor.execute(query)
records = cursor.fetchall()

db_title_list = [x[0] for x in records]

searchable_df = df[~df['show_title'].isin(db_title_list)]

for index, row in searchable_df.iterrows():
	search_list = search_movie(row['show_title'])

	movies_to_validate = []
	for movie in search_list['results']:
		if movie['title'].lower() == row['show_title'].lower():
			movies_to_validate.append(movie['id'])
	
	if len(search_list['results']) == 0:
		print('NO RESULTS FOUND FOR MOVIE ' + row['show_title'] + '!!!')
		continue
	
	if len(movies_to_validate) == 0:
		tmdb_id = search_list['results'][0]['id']
		movie_details = extract_movie_details(tmdb_id)
		save(tmdb_id, movie_details, row['show_title'])
		continue
	
	if len(movies_to_validate) == 1:
		tmdb_id = movies_to_validate[0]
		movie_details = extract_movie_details(tmdb_id)
		save(tmdb_id, movie_details, row['show_title'])
		continue
	
	distance_days = None
	result = None
	tmdb_id = None
	for movie_id in movies_to_validate:
		movie_details = extract_movie_details(movie_id)

		if result is None:
			result = movie_details
			tmdb_id = movie_id

		if movie_details['release_date_streaming'] is not None:
			streaming_release_date = datetime.strptime(movie_details['release_date_streaming'], "%Y-%m-%dT%H:%M:%S.%f%z").replace(tzinfo=None)
			if row['date_release'] is not np.nan:
				report_date_release = row['date_release']
				if isinstance(report_date_release, str):
					report_date_release = datetime.strptime(row['date_release'], "%Y-%m-%d")

				difference = abs((streaming_release_date - report_date_release).days)

				if distance_days is None:
					distance_days = difference
					result = movie_details
					tmdb_id = movie_id
				else:
					if difference < distance_days:
						distance_days = difference
						result = movie_details
						tmdb_id = movie_id
			else:
				difference = abs((streaming_release_date - datetime(2023, 7, 2)).days)
				if distance_days is None:
					distance_days = difference
					result = movie_details
					tmdb_id = movie_id
				else:
					if difference < distance_days:
						distance_days = difference
						result = movie_details
						tmdb_id = movie_id
	
	save(tmdb_id, result, row['show_title'])
		

# 2021-11-05 00:00:00
# datetime.strptime(a, "%Y-%m-%dT%H:%M:%S.%f%z")
