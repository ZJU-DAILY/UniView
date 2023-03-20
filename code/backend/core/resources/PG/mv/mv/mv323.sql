create materialized view if not exists mv323 as select keyword.id AS id, keyword.keyword AS keyword, movie_keyword.movie_id AS movie_id
 from keyword,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('superhero', 'marvel-comics', 'based-on-comic', 'fight'))