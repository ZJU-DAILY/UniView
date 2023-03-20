select info_type.info AS info_type_info_0, movie_info.info AS movie_info_info_1, movie_info.movie_id AS movie_id, movie_info.note AS note
 from movie_info,info_type
 where (info_type.id = movie_info.info_type_id) And (movie_info.note like '%internet%')  And  (movie_info.info like 'USA:% 200%') And (movie_info.info_type_id = info_type.id) And (info_type.info = 'release dates')