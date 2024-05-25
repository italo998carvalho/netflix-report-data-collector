CREATE TABLE tmdb_movie (
  tmdb_id INT PRIMARY KEY UNIQUE,
  report_title VARCHAR NOT NULL UNIQUE,
  tmdb_title VARCHAR,
  budget NUMERIC(12,2),
  release_date_streaming DATE,
  country_release_date_streaming VARCHAR,
  all_countries_streaming VARCHAR,
  release_date_theater DATE,
  country_release_date_theater VARCHAR,
  all_countries_theater VARCHAR,
  revenue NUMERIC(12,2)
);

CREATE TABLE imdb_movie (
  tmdb_id INT NOT NULL,
  imdb_id VARCHAR NOT NULL UNIQUE,
  imdb_title VARCHAR,
  rating FLOAT,
  budget VARCHAR,
  gross_worldwide VARCHAR,
  gross_us_canada VARCHAR,
  openning_week_us_canada VARCHAR,
  CONSTRAINT fk_tmdb_id
      FOREIGN KEY(tmdb_id)
      REFERENCES tmdb_movie(tmdb_id)
);

CREATE TABLE mojo_movie (
  imdb_id VARCHAR NOT NULL,
  mojo_title VARCHAR,
  performance_domestic VARCHAR,
  performance_international VARCHAR,
  performance_worldwide VARCHAR,
  mpaa VARCHAR,
  genres VARCHAR,
  CONSTRAINT fk_imdb_id
      FOREIGN KEY(imdb_id)
      REFERENCES imdb_movie(imdb_id)
);
