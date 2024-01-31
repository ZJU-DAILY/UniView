select info_type.id AS info_type_id_0, info_type.info AS info_type_info_1, keyword.id AS keyword_id_2, keyword.keyword AS keyword, movie_info_idx.info AS movie_info_idx_info_4, movie_info_idx.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from movie_keyword,movie_info_idx,title,info_type,keyword
 where (movie_keyword.keyword_id = keyword.id) And (title.id = movie_keyword.movie_id) And (movie_info_idx.movie_id = title.id) And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'votes') And (keyword.keyword in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital'))