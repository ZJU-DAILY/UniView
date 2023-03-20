create materialized view if not exists mv62 engine = MergeTree() order by tuple() POPULATE as 
select movie_info.info AS movie_info_info_0, info_type.info AS info_type_info_1, movie_info.movie_id AS movie_id, info_type.id AS id
 from movie_info,info_type
 where (info_type.id = movie_info.info_type_id) 
 And ((movie_info.info = 'Sweden') Or (movie_info.info =  'Norway') Or (movie_info.info =  'Germany') Or (movie_info.info =  'Denmark') Or (movie_info.info =  'Swedish') Or (movie_info.info =  'Danish') Or (movie_info.info =  'Norwegian') Or (movie_info.info =  'German') Or (movie_info.info =  'USA') Or (movie_info.info =  'American'))