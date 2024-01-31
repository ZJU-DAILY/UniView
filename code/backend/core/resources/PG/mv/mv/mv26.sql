create materialized view if not exists mv26 as select info_type.id AS id, info_type.info AS info_type_info_1, movie_info_idx.info AS movie_info_idx_info_2, movie_info_idx.movie_id AS movie_id
 from movie_info_idx,info_type
 where (movie_info_idx.info_type_id = info_type.id) And (movie_info_idx.info > '9.0') And (info_type.info = 'rating')