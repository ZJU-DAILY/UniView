select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, complete_cast.movie_id AS complete_cast_movie_id_2, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_4, info_type.info AS info_type_info_5, keyword.id AS keyword_id_6, keyword.keyword AS keyword, movie_info_idx.info AS movie_info_idx_info_8, movie_keyword.movie_id AS movie_keyword_movie_id_9, title.production_year AS production_year, title.title AS title
 from info_type,title,comp_cast_type,movie_keyword,keyword,complete_cast,movie_info_idx
 where (info_type.id = movie_info_idx.info_type_id) And (title.id = movie_keyword.movie_id) And (complete_cast.movie_id = title.id) And (movie_info_idx.movie_id = title.id) And (comp_cast_type.id = complete_cast.status_id) And (comp_cast_type.id = complete_cast.subject_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital'))