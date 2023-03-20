create materialized view if not exists mv277 as select keyword.id AS id, keyword.keyword AS keyword, movie_keyword.movie_id AS movie_id
 from keyword,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('nerd', 'loner', 'alienation', 'dignity'))