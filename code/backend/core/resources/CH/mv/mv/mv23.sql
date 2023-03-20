create materialized view if not exists mv23 engine = MergeTree() order by tuple() POPULATE as 
select movie_info_idx.info AS movie_info_idx_info_0, info_type.info AS info_type_info_1, movie_info_idx.movie_id AS movie_id, info_type.id AS id
 from info_type,movie_info_idx
 where (info_type.id = movie_info_idx.info_type_id) And (info_type.info = 'rating') And (movie_info_idx.info > '2.0')