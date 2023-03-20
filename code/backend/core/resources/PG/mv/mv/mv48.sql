create materialized view if not exists mv48 as select keyword.id AS id, keyword.keyword AS keyword, movie_keyword.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from keyword,title,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence')) And (title.id = movie_keyword.movie_id)