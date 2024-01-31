select cast_info.movie_id AS movie_id, cast_info.person_id AS person_id, cast_info.person_role_id AS person_role_id, char_name.name AS name, comp_cast_type.id AS comp_cast_type_id_4, comp_cast_type.kind AS kind, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_7, info_type.info AS info_type_info_8, movie_info_idx.info AS movie_info_idx_info_9
 from char_name,movie_info_idx,name,comp_cast_type,complete_cast,cast_info,info_type
 where (char_name.id = cast_info.person_role_id) And (complete_cast.movie_id = movie_info_idx.movie_id) And (movie_info_idx.info_type_id = info_type.id) And (name.id = cast_info.person_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind like '%complete%') And (cast_info.movie_id = complete_cast.movie_id) And (info_type.info = 'rating')