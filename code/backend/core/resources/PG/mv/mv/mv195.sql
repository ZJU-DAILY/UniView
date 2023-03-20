create materialized view if not exists mv195 as select aka_title.movie_id AS movie_id, movie_info.info AS info, movie_info.info_type_id AS info_type_id, movie_info.note AS note, movie_keyword.keyword_id AS keyword_id, title.production_year AS production_year, title.title AS title
 from movie_info,aka_title,title,movie_keyword
 where (movie_info.note like '%internet%')  And  (movie_info.info like 'USA:% 200%') And (title.id = movie_info.movie_id) And (aka_title.movie_id = title.id) And (movie_keyword.movie_id = title.id)