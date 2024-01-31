create materialized view if not exists mv343 as select cast_info.movie_id AS cast_info_movie_id_0, cast_info.person_id AS person_id, cast_info.person_role_id AS person_role_id, char_name.name AS name, comp_cast_type.id AS comp_cast_type_id_4, comp_cast_type.kind AS comp_cast_type_kind_5, info_type.id AS info_type_id_6, info_type.info AS info_type_info_7, keyword.id AS keyword_id_8, keyword.keyword AS keyword, kind_type.id AS kind_type_id_10, kind_type.kind AS kind_type_kind_11, movie_info_idx.info AS movie_info_idx_info_12, movie_keyword.movie_id AS movie_keyword_movie_id_13, title.production_year AS production_year, title.title AS title
 from char_name,movie_info_idx,name,keyword,title,comp_cast_type,complete_cast,kind_type,cast_info,info_type,movie_keyword
 where (char_name.id = cast_info.person_role_id) And (complete_cast.movie_id = movie_info_idx.movie_id) And (movie_info_idx.info_type_id = info_type.id) And (name.id = cast_info.person_id) And (keyword.id = movie_keyword.keyword_id) And (title.id = cast_info.movie_id) And (movie_keyword.movie_id = title.id) And (kind_type.id = title.kind_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind like '%complete%') And (cast_info.movie_id = complete_cast.movie_id) And (comp_cast_type.id = complete_cast.subject_id) And (movie_keyword.movie_id = cast_info.movie_id) And (info_type.info = 'rating')