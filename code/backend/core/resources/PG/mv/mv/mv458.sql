create materialized view if not exists mv458 as select cast_info.movie_id AS cast_info_movie_id_0, cast_info.note AS note, cast_info.person_id AS person_id, comp_cast_type.id AS comp_cast_type_id_3, comp_cast_type.kind AS kind, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_6, info_type.info AS info_type_info_7, keyword.id AS keyword_id_8, keyword.keyword AS keyword, movie_info_idx.info AS movie_info_idx_info_10, movie_keyword.movie_id AS movie_keyword_movie_id_11, title.production_year AS production_year, title.title AS title
 from movie_keyword,movie_info_idx,title,comp_cast_type,complete_cast,cast_info,info_type,keyword
 where (movie_keyword.keyword_id = keyword.id) And (title.id = movie_keyword.movie_id) And (movie_info_idx.movie_id = title.id) And (info_type.id = movie_info_idx.info_type_id) And (cast_info.movie_id = title.id) And (complete_cast.movie_id = title.id) And (comp_cast_type.id = complete_cast.status_id) And (comp_cast_type.id = complete_cast.subject_id) And (cast_info.note in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')) And (keyword.keyword in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital'))