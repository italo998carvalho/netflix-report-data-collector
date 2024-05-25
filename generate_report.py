import pandas as pd
import psycopg2

conn = psycopg2.connect(
  database='movies',
  host='localhost',
  user='user',
  password='p@ss',
  port='5432'
)

cursor = conn.cursor()

query = """
	SELECT
		*
	FROM
		tmdb_movie tmdb
	INNER JOIN
		imdb_movie imdb ON tmdb.tmdb_id = imdb.tmdb_id
		INNER JOIN mojo_movie mojo ON mojo.imdb_id = imdb.imdb_id;
"""

cursor.execute(query)
records = cursor.fetchall()

columns = [
	'tmdb_id',
	'report_title',
	'tmdb_title',
	'tmdb_budget',
	'release_date_streaming',
	'release_date_streaming_country',
	'all_countries_streaming',
	'release_date_theater',
	'release_date_theater_country',
	'all_countries_on_theater',
	'revenue',
	'imdb_id',
	'imdb_title',
	'rating',
	'imdb_budget',
	'gross_worldwide',
	'gross_us_canada',
	'openning_week_us_canada',
	'mojo_performance_domestic',
	'mojo_performance_international',
	'mojo_performance_worldwide',
	'mpaa',
	'genres'
]
movies = pd.DataFrame(columns = columns)

for row in records:
	tmdb_id = row[0]
	report_title = row[1]
	tmdb_title = row[2]
	tmdb_budget = row[3]
	release_date_streaming = row[4]
	release_date_streaming_country = row[5]
	all_countries_streaming = row[6]
	release_date_theater = row[7]
	release_date_theater_country = row[8]
	all_countries_theater = row[9]
	revenue = row[10]
	imdb_id = row[12]
	imdb_title = row[13]
	rating = row[14]
	imdb_budget = row[15]
	gross_worldwide = row[16]
	gross_us_canada = row[17]
	openning_week_us_canada = row[18]
	mojo_performance_domestic = row[21]
	mojo_performance_international = row[22]
	mojo_performance_worldwide = row[23]
	mpaa = row[24]
	genres = row[25]

	data = {
		'tmdb_id': tmdb_id,
		'report_title': report_title,
		'tmdb_title': tmdb_title,
		'tmdb_budget': tmdb_budget,
		'release_date_streaming': release_date_streaming,
		'release_date_streaming_country': release_date_streaming_country,
		'all_countries_streaming': all_countries_streaming,
		'release_date_theater': release_date_theater,
		'release_date_theater_country': release_date_theater_country,
		'all_countries_on_theater': all_countries_theater,
		'revenue': revenue,
		'imdb_id': imdb_id,
		'imdb_title': imdb_title,
		'rating': rating,
		'imdb_budget': imdb_budget,
		'gross_worldwide': gross_worldwide,
		'gross_us_canada': gross_us_canada,
		'openning_week_us_canada': openning_week_us_canada,
		'mojo_performance_domestic': mojo_performance_domestic,
		'mojo_performance_international': mojo_performance_international,
		'mojo_performance_worldwide': mojo_performance_worldwide,
		'mpaa': mpaa,
		'genres': genres
	}

	movies.loc[len(movies)] = data

movies.to_excel('results/result_final.xlsx')


