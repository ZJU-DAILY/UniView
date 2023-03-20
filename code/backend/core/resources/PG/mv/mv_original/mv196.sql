select aka_title.movie_id AS movie_id, info_type.id AS id, info_type.info AS info_type_info_2, movie_info.info AS movie_info_info_3, movie_info.note AS note, movie_keyword.keyword_id AS keyword_id, title.production_year AS production_year, title.title AS title
 from info_type,title,movie_keyword,aka_title,movie_info
 where (movie_info.info_type_id = info_type.id) And (info_type.info = 'release dates') And (title.id = movie_info.movie_id) And (aka_title.movie_id = title.id) And (movie_keyword.movie_id = title.id) And (movie_info.note like '%internet%')  And  (movie_info.info like 'USA:% 200%')