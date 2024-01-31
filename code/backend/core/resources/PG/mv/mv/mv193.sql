create materialized view if not exists mv193 as select movie_info.info AS info, movie_info.info_type_id AS info_type_id, movie_info.movie_id AS movie_id, movie_info.note AS note, title.production_year AS production_year, title.title AS title
 from title,movie_info
 where (title.id = movie_info.movie_id) And (movie_info.note like '%internet%')  And  (movie_info.info like 'USA:% 200%')