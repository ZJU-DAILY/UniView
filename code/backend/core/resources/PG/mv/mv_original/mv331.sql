select cast_info.movie_id AS cast_info_movie_id_0, cast_info.person_id AS person_id, cast_info.person_role_id AS person_role_id, comp_cast_type.id AS comp_cast_type_id_3, comp_cast_type.kind AS comp_cast_type_kind_4, complete_cast.movie_id AS complete_cast_movie_id_5, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_7, info_type.info AS info_type_info_8, keyword.id AS keyword_id_9, keyword.keyword AS keyword, kind_type.id AS kind_type_id_11, kind_type.kind AS kind_type_kind_12, movie_info_idx.info AS movie_info_idx_info_13, title.production_year AS production_year, title.title AS title
 from cast_info,info_type,title,comp_cast_type,movie_keyword,keyword,kind_type,complete_cast,movie_info_idx
 where (cast_info.movie_id = title.id) And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'rating') And (title.id = movie_keyword.movie_id) And (kind_type.id = title.kind_id) And (comp_cast_type.id = complete_cast.status_id) And (comp_cast_type.id = complete_cast.subject_id) And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_info_idx.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('superhero', 'marvel-comics', 'based-on-comic', 'fight')) And (movie_info_idx.info > '8.0')