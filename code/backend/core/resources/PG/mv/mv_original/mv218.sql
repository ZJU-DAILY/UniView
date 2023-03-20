select info_type.id AS id, info_type.info AS info, movie_info.movie_id AS movie_id, movie_info.note AS note, title.production_year AS production_year, title.title AS title
 from movie_info,info_type,title
 where (info_type.id = movie_info.info_type_id) And (movie_info.note like '%internet%') And (title.id = movie_info.movie_id) And (info_type.info = 'release dates')