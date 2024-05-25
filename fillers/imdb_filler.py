import requests
import psycopg2
import time
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
	movie_details_url = f"http://imdb.com/title/{imdb_id}/?ref_=fn_tt_tt_1"
	movie_details = requests.get(movie_details_url, headers={'User-Agent': 'Chrome'})
	details_status = movie_details.status_code
	while details_status != 200:
		time.sleep(50/1000)
		movie_details = requests.get(movie_details_url, headers={'User-Agent': 'Chrome'})
		details_status = movie_details.status_code
	
	return movie_details.content

def handle_field_to_db(field):
	if field is None:
		field = 'NULL'
	else:
		field = "'" + str(field) + "'"
	
	return field

def save(imdb_id, data):
	imdb_id = handle_field_to_db(imdb_id)
	imdb_title = handle_field_to_db(data['imdb_title'].replace("'", "''"))
	rating = handle_field_to_db(data['rating'])
	budget = handle_field_to_db(data['budget'])
	openning_week_us_canada = handle_field_to_db(data['openning_week_us_canada'])
	gross_worldwide = handle_field_to_db(data['gross_worldwide'])
	gross_us_canada = handle_field_to_db(data['gross_us_canada'])

	query = f"""
		UPDATE
			imdb_movie
		SET
			imdb_title={imdb_title},
			rating={rating},
			budget={budget},
			openning_week_us_canada={openning_week_us_canada},
			gross_worldwide={gross_worldwide},
			gross_us_canada={gross_us_canada}
		WHERE
			imdb_id={imdb_id}
	"""
	try:
		cursor.execute(query)
		conn.commit()

		print('Inserted ' + title + '!!!')
	except Exception as e:
		conn.rollback()

		print('Could not insert movie ' + title + ' with id ' + str(imdb_id) + ': ' + str(e))

def extract_box_office_details(soup):
	budget, openingWeekendUSnCanada, grossUSnCanada, grossWorldwide = None, None, None, None
	try:
		box_office = soup.find('div', {'data-testid': 'title-boxoffice-section'}).find('ul')
	except:
		pass
	
	try:
		budget = box_office.find('li', {'data-testid': 'title-boxoffice-budget'}).find('li', {'role': 'presentation'}).text
	except:
		pass

	try:
		openingWeekendUSnCanada = box_office.find('li', {'data-testid': 'title-boxoffice-openingweekenddomestic'}).find('li', {'role': 'presentation'}).text
	except:
		pass

	try:
		grossUSnCanada = box_office.find('li', {'data-testid': 'title-boxoffice-grossdomestic'}).find('li', {'role': 'presentation'}).text
	except:
		pass

	try:
		grossWorldwide = box_office.find('li', {'data-testid': 'title-boxoffice-cumulativeworldwidegross'}).find('li', {'role': 'presentation'}).text
	except:
		pass

	return {
		'budget': budget,
		'openingWeekendUSnCanada': openingWeekendUSnCanada,
		'grossUSnCanada': grossUSnCanada,
		'grossWorldwide': grossWorldwide
	}

def extract_rating(soup):
	rating = None
	try:
		rating = soup.find('div', {'data-testid': 'hero-rating-bar__aggregate-rating__score'}).text
		rating = float(rating.split('/')[0])
	except:
		pass

	return rating

def get_missing_random_imdb_id():
	query = "SELECT imdb_id FROM imdb_movie WHERE imdb_title IS NULL ORDER BY RANDOM() LIMIT 1;"

	cursor.execute(query)
	return cursor.fetchone()

while True:
	record = get_missing_random_imdb_id()
	if record is None:
		break

	imdb_id = record[0]

	soup = BeautifulSoup(get_movie_data(imdb_id), "html.parser")

	title = soup.title.text

	rating = extract_rating(soup)

	box_office_details = extract_box_office_details(soup)
	budget = box_office_details['budget']
	openingWeekendUSnCanada = box_office_details['openingWeekendUSnCanada']
	grossUSnCanada = box_office_details['grossUSnCanada']
	grossWorldwide = box_office_details['grossWorldwide']

	data = {
		'imdb_title': title,
		'rating': rating,
		'budget': budget,
		'openning_week_us_canada': openingWeekendUSnCanada,
		'gross_worldwide': grossWorldwide,
		'gross_us_canada': grossUSnCanada
	}
	save(imdb_id, data)

print('FINISHED!!!')
	

