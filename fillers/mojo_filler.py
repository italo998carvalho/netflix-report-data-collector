import requests
import time
import psycopg2
from bs4 import BeautifulSoup

conn = psycopg2.connect(
  database='movies',
  host='localhost',
  user='user',
  password='p@ss',
  port='5432'
)

cursor = conn.cursor()

def get_movie_data(imdb_id):
	movie_details_url = f"https://www.boxofficemojo.com/title/{imdb_id}/?ref_=bo_rl_ti"
	movie_details = requests.get(movie_details_url, headers={'User-Agent': 'Chrome'})
	details_status = movie_details.status_code
	while details_status != 200:
		time.sleep(50/1000)
		movie_details = requests.get(movie_details_url, headers={'User-Agent': 'Chrome'})
		details_status = movie_details.status_code

	return movie_details.content

def extract_release_values(soup):
	release_values_table = soup.find('div', {'class': 'mojo-performance-summary-table'})
	values = release_values_table.find_all('div', {'class': 'a-section a-spacing-none'})
	domestic, international, worldwide = None, None, None

	try:
		domestic = values[0].find('span', {'class': 'money'}).text
	except:
		pass

	try:
		international = values[1].find('span', {'class': 'money'}).text
	except:
		pass

	try:
		worldwide = values[2].find('span', {'class': 'money'}).text
	except:
		pass

	return {
		'domestic': domestic,
		'international': international,
		'worldwide': worldwide
	}

def extract_summary(soup):
	summary_values = soup.find('div', {'class': 'mojo-summary-values'})
	mpaa, genres = None, None

	try:
		mpaa_tag = summary_values.find('span', string='MPAA').parent
		mpaa = mpaa_tag.find_all('span')[1].text
	except:
		pass

	try:
		genres_tag = summary_values.find('span', string='Genres').parent
		genres = genres_tag.find_all('span')[1].text
		genres = ", ".join(genres.split())
	except:
		pass

	return {
		'mpaa': mpaa,
		'genres': genres
	}

def handle_field_to_db(field):
	if field is None:
		field = 'NULL'
	else:
		field = "'" + str(field) + "'"
	
	return field

def save(imdb_id, data):	
	title = handle_field_to_db(data['mojo_title'].replace("'", "''"))
	performance_domestic = handle_field_to_db(data['performance_domestic'])
	performance_international = handle_field_to_db(data['performance_international'])
	performance_worldwide = handle_field_to_db(data['performance_worldwide'])
	mpaa = handle_field_to_db(data['mpaa'])
	genres = handle_field_to_db(data['genres'])

	query = f"""
		UPDATE
			mojo_movie
		SET
			mojo_title={title},
			performance_domestic={performance_domestic},
			performance_international={performance_international},
			performance_worldwide={performance_worldwide},
			mpaa={mpaa},
			genres={genres}
		WHERE
			imdb_id='{imdb_id}'
	"""
	try:
		cursor.execute(query)
		conn.commit()

		print('Inserted ' + title + '!!!')
	except Exception as e:
		conn.rollback()

		print('Could not insert mojo movie ' + title + ' with id ' + str(imdb_id) + ': ' + str(e))

def get_missing_random_imdb_id():
	query = "SELECT imdb_id FROM mojo_movie WHERE mojo_title IS NULL ORDER BY RANDOM() LIMIT 1;"

	cursor.execute(query)
	return cursor.fetchone()

while True:
	record = get_missing_random_imdb_id()
	if record is None:
		break

	imdb_id = record[0]

	soup = BeautifulSoup(get_movie_data(imdb_id), "html.parser")

	title = soup.title.text

	release_values = extract_release_values(soup)
	domestic = release_values['domestic']
	international = release_values['international']
	worldwide = release_values['worldwide']

	summary = extract_summary(soup)
	mpaa = summary['mpaa']
	genres = summary['genres']

	data = {
		'mojo_title': title,
		'performance_domestic': domestic,
		'performance_international': international,
		'performance_worldwide': worldwide,
		'mpaa': mpaa,
		'genres': genres
	}
	save(imdb_id, data)

print('FINISHED!!!')
