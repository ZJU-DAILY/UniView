create materialized view if not exists mv145 as select keyword.id AS id, keyword.keyword AS keyword, movie_keyword.movie_id AS movie_id, movie_link.link_type_id AS link_type_id
 from movie_link,keyword,movie_keyword
 where (movie_keyword.movie_id = movie_link.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('sequel', 'revenge', 'based-on-novel'))