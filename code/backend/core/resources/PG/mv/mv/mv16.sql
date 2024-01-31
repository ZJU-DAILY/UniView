create materialized view if not exists mv16 as select keyword.id AS id, keyword.keyword AS keyword, movie_keyword.movie_id AS movie_id, title.episode_nr AS episode_nr, title.title AS title
 from title,keyword,movie_keyword
 where (title.id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'character-name-in-title')