create materialized view if not exists mv13 as select keyword.id AS id, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.movie_id AS movie_id
 from movie_companies,keyword,movie_keyword
 where (movie_companies.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'character-name-in-title')