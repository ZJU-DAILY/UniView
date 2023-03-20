create materialized view if not exists mv13 engine = MergeTree() order by tuple() POPULATE as 
select movie_info.info AS info, movie_keyword.movie_id AS movie_id, keyword.id AS id, keyword.keyword AS keyword
 from movie_keyword,movie_info,keyword
 where (movie_keyword.movie_id = movie_info.movie_id) And (keyword.id = movie_keyword.keyword_id) And ((movie_info.info = 'Bulgaria')) And ((keyword.keyword like '%sequel%'))