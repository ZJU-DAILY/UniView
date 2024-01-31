select info_type.id AS info_type_id_0, info_type.info AS info_type_info_1, keyword.id AS keyword_id_2, keyword.keyword AS keyword, kind_type.id AS kind_type_id_4, kind_type.kind AS kind, movie_info.info AS movie_info_info_6, movie_info.info_type_id AS info_type_id, movie_info.movie_id AS movie_info_movie_id_8, movie_info_idx.info AS movie_info_idx_info_9, movie_keyword.movie_id AS movie_keyword_movie_id_10, title.production_year AS production_year, title.title AS title
 from movie_keyword,movie_info_idx,movie_info,keyword,title,info_type,kind_type
 where (movie_keyword.keyword_id = keyword.id) And (title.id = movie_keyword.movie_id) And (movie_info_idx.movie_id = title.id) And (movie_info_idx.info_type_id = info_type.id) And (movie_info_idx.info < '8.5') And (movie_info.movie_id = title.id) And (movie_info.info_type_id = info_type.id) And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American')) And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence')) And (kind_type.id = title.kind_id) And (info_type.info = 'countries') And (info_type.info = 'rating')