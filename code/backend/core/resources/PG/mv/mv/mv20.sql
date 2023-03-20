create materialized view if not exists mv20 as select keyword.id AS id, keyword.keyword AS keyword, movie_info.info AS info, movie_info.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from keyword,movie_info,title,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword like '%sequel%') And (movie_info.movie_id = title.id) And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German')) And (title.id = movie_keyword.movie_id)