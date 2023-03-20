select cast_info.movie_id AS cast_info_movie_id_0, cast_info.person_id AS person_id, cast_info.person_role_id AS person_role_id, char_name.name AS char_name_name_3, comp_cast_type.id AS comp_cast_type_id_4, comp_cast_type.kind AS comp_cast_type_kind_5, complete_cast.movie_id AS complete_cast_movie_id_6, info_type.id AS info_type_id_7, info_type.info AS info_type_info_8, keyword.id AS keyword_id_9, keyword.keyword AS keyword, kind_type.id AS kind_type_id_11, kind_type.kind AS kind_type_kind_12, movie_info_idx.info AS movie_info_idx_info_13, name.name AS name_name_14, title.production_year AS production_year, title.title AS title
 from cast_info,title,info_type,comp_cast_type,movie_keyword,name,keyword,kind_type,complete_cast,movie_info_idx,char_name
 where (cast_info.movie_id = title.id) And (char_name.id = cast_info.person_role_id) And (name.id = cast_info.person_id) And (title.id = movie_keyword.movie_id) And (kind_type.id = title.kind_id) And (info_type.id = movie_info_idx.info_type_id) And (comp_cast_type.id = complete_cast.status_id) And (complete_cast.subject_id = comp_cast_type.id) And (comp_cast_type.kind = 'cast') And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw', 'laser')) And (movie_info_idx.movie_id = title.id) And (movie_info_idx.info > '7.0')