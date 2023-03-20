create materialized view if not exists mv22 engine = MergeTree() order by tuple() POPULATE as 
select title.id AS title_id_0, movie_info_idx.info AS movie_info_idx_info_1, movie_keyword.movie_id AS movie_keyword_movie_id_2, keyword.id AS keyword_id_3, info_type.info AS info_type_info_4, keyword.keyword AS keyword, movie_info_idx.movie_id AS movie_info_idx_movie_id_6, info_type.id AS info_type_id_7, title.production_year AS production_year, title.title AS title
 from title,movie_info_idx,info_type,movie_keyword,keyword
 where (title.production_year > 1990) And title.id = movie_info_idx.movie_id  And  title.id = movie_keyword.movie_id And (movie_info_idx.info > '9.0') And  movie_keyword.movie_id = movie_info_idx.movie_id And (info_type.id = movie_info_idx.info_type_id) And (info_type.info = 'rating') And (keyword.id = movie_keyword.keyword_id) And ((keyword.keyword like '%sequel%'))