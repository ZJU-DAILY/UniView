create materialized view if not exists mv235 as select info_type.id AS id, info_type.info AS info_type_info_1, movie_info.info AS movie_info_info_2, movie_info.movie_id AS movie_id, movie_info_idx.info AS movie_info_idx_info_4, movie_info_idx.info_type_id AS info_type_id
 from info_type,movie_info,movie_info_idx
 where (movie_info.info_type_id = info_type.id) And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'budget') And (info_type.info = 'votes') And (movie_info.movie_id = movie_info_idx.movie_id)