create materialized view if not exists mv342 as select cast_info.movie_id AS cast_info_movie_id_0, cast_info.person_id AS person_id, cast_info.person_role_id AS person_role_id, char_name.name AS name, comp_cast_type.id AS comp_cast_type_id_4, comp_cast_type.kind AS comp_cast_type_kind_5, complete_cast.movie_id AS complete_cast_movie_id_6, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_8, info_type.info AS info_type_info_9, keyword.id AS keyword_id_10, keyword.keyword AS keyword, kind_type.id AS kind_type_id_12, kind_type.kind AS kind_type_kind_13, movie_info_idx.info AS movie_info_idx_info_14, title.production_year AS production_year, title.title AS title
 from info_type,cast_info,name,comp_cast_type,movie_keyword,title,keyword,kind_type,complete_cast,movie_info_idx,char_name
 where (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'rating') And (cast_info.movie_id = title.id) And (char_name.id = cast_info.person_role_id) And (name.id = cast_info.person_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind like '%complete%') And (movie_keyword.movie_id = title.id) And (movie_keyword.movie_id = cast_info.movie_id) And (keyword.id = movie_keyword.keyword_id) And (title.id = movie_keyword.movie_id) And (kind_type.id = title.kind_id) And (cast_info.movie_id = complete_cast.movie_id) And (complete_cast.movie_id = movie_info_idx.movie_id)