create materialized view if not exists mv47 as select keyword.id AS id, keyword.keyword AS keyword, movie_keyword.movie_id AS movie_id
 from keyword,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence'))