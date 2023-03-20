create materialized view if not exists mv336 as select cast_info.movie_id AS movie_id, cast_info.person_id AS person_id, cast_info.person_role_id AS person_role_id, comp_cast_type.id AS comp_cast_type_id_3, comp_cast_type.kind AS kind, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_6, info_type.info AS info_type_info_7, movie_info_idx.info AS movie_info_idx_info_8
 from info_type,cast_info,comp_cast_type,complete_cast,movie_info_idx
 where (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'rating') And (cast_info.movie_id = complete_cast.movie_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind like '%complete%') And (complete_cast.movie_id = movie_info_idx.movie_id)