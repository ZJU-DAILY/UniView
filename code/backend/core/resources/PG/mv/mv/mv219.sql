create materialized view if not exists mv219 as select aka_title.movie_id AS movie_id, aka_title.title AS aka_title_title_1, info_type.id AS id, info_type.info AS info, movie_info.note AS note, title.production_year AS production_year, title.title AS title_title_6
 from info_type,movie_info,aka_title,title
 where (info_type.id = movie_info.info_type_id) And (info_type.info = 'release dates') And (movie_info.note like '%internet%') And (title.id = movie_info.movie_id) And (aka_title.movie_id = title.id)