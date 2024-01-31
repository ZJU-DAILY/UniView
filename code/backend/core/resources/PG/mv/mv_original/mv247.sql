select info_type.id AS id, info_type.info AS info_type_info_1, movie_info.info AS movie_info_info_2, movie_info.movie_id AS movie_id
 from movie_info,info_type
 where (info_type.id = movie_info.info_type_id) And (movie_info.info IS NOT NULL)  And  (((movie_info.info = '__LIKE__Japan:%2007%')) Or ((movie_info.info = '__LIKE__USA:%2008%'))) And (info_type.info = 'release dates')