select movie_info.info AS movie_info_info_0, movie_info_idx.info AS movie_info_idx_info_1, info_type.info AS info_type_info_2, movie_info.movie_id AS movie_id, info_type.id AS id, movie_info.info_type_id AS info_type_id
 from movie_info_idx,info_type,movie_info
 where (movie_info.movie_id = movie_info_idx.movie_id) And (info_type.id = movie_info_idx.info_type_id) And (movie_info_idx.info < '8.5') And info_type.id = movie_info.info_type_id  And ((movie_info.info = 'Sweden') Or (movie_info.info =  'Norway') Or (movie_info.info =  'Germany') Or (movie_info.info =  'Denmark') Or (movie_info.info =  'Swedish') Or (movie_info.info =  'Danish') Or (movie_info.info =  'Norwegian') Or (movie_info.info =  'German') Or (movie_info.info =  'USA') Or (movie_info.info =  'American'))