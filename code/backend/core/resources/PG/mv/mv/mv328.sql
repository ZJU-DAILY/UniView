create materialized view if not exists mv328 as select complete_cast.movie_id AS movie_id, complete_cast.status_id AS status_id, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_3, info_type.info AS info_type_info_4, keyword.id AS keyword_id_5, keyword.keyword AS keyword, kind_type.id AS kind_type_id_7, kind_type.kind AS kind, movie_info_idx.info AS movie_info_idx_info_9, title.id AS title_id_10, title.production_year AS production_year, title.title AS title
 from info_type,title,movie_keyword,keyword,kind_type,complete_cast,movie_info_idx
 where (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'rating') And (title.id = movie_keyword.movie_id) And (kind_type.id = title.kind_id) And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_info_idx.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('superhero', 'marvel-comics', 'based-on-comic', 'fight')) And (movie_info_idx.info > '8.0')