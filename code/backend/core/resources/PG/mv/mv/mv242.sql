create materialized view if not exists mv242 as select cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.person_id AS person_id, info_type.id AS id, info_type.info AS info_type_info_4, movie_info.info AS movie_info_info_5, movie_info_idx.info AS movie_info_idx_info_6, title.title AS title
 from info_type,cast_info,title,movie_info_idx,movie_info
 where (info_type.id = movie_info.info_type_id) And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'genres') And (info_type.info = 'votes') And (cast_info.movie_id = title.id) And (cast_info.note in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')) And (title.id = movie_info_idx.movie_id) And (movie_info.movie_id = title.id) And (movie_info.movie_id = movie_info_idx.movie_id) And (movie_info.info in ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War'))