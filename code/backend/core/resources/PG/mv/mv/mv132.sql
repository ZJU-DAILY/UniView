create materialized view if not exists mv132 as select movie_keyword.keyword_id AS keyword_id, movie_keyword.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from title,movie_keyword
 where (movie_keyword.movie_id = title.id) And (title.title like '%Money%')  And  (title.production_year = 1998)