create materialized view if not exists mv479 as select cast_info.movie_id AS cast_info_movie_id_0, cast_info.note AS note, cast_info.person_id AS person_id, comp_cast_type.id AS comp_cast_type_id_3, comp_cast_type.kind AS kind, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_6, info_type.info AS info_type_info_7, movie_info.info AS movie_info_info_8, movie_info.movie_id AS movie_info_movie_id_9, movie_info_idx.info AS movie_info_idx_info_10, movie_info_idx.info_type_id AS info_type_id, name.gender AS gender, name.name AS name, title.title AS title
 from movie_info_idx,name,movie_info,title,comp_cast_type,complete_cast,cast_info,info_type
 where (title.id = movie_info_idx.movie_id) And (cast_info.movie_id = movie_info_idx.movie_id) And (movie_info_idx.movie_id = complete_cast.movie_id) And (name.id = cast_info.person_id) And (movie_info.movie_id = title.id) And (movie_info.info in ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')) And (info_type.id = movie_info.info_type_id) And (complete_cast.status_id = comp_cast_type.id) And (complete_cast.subject_id = comp_cast_type.id) And (comp_cast_type.kind = 'cast') And (comp_cast_type.kind = 'complete+verified') And (cast_info.note in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)'))