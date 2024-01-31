select aka_title.movie_id AS movie_id, info_type.id AS id, info_type.info AS info_type_info_2, movie_info.info AS movie_info_info_3, movie_info.note AS note
 from aka_title,movie_info,info_type
 where (movie_info.movie_id = aka_title.movie_id) And (info_type.id = movie_info.info_type_id) And (movie_info.info IS NOT NULL)  And  (movie_info.note like '%internet%')  And  (((movie_info.info like 'USA:% 199%')) Or ((movie_info.info like 'USA:% 200%'))) And (info_type.info = 'release dates')