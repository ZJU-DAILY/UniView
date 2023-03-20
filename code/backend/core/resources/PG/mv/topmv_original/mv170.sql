select info_type.id AS id, info_type.info AS info_type_info_1, movie_info_idx.info AS movie_info_idx_info_2, movie_info_idx.movie_id AS movie_id
 from info_type,movie_info_idx
 where (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'rating')