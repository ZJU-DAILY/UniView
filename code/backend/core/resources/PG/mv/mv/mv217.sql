create materialized view if not exists mv217 as select info_type.id AS id, info_type.info AS info, movie_info.movie_id AS movie_id, movie_info.note AS note
 from movie_info,info_type
 where (info_type.id = movie_info.info_type_id) And (movie_info.note like '%internet%') And (info_type.info = 'release dates')