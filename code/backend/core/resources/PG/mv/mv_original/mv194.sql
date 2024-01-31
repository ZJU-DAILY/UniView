select aka_title.movie_id AS movie_id, movie_info.info AS info, movie_info.info_type_id AS info_type_id, movie_info.note AS note, title.production_year AS production_year, title.title AS title
 from aka_title,title,movie_info
 where (aka_title.movie_id = title.id) And (title.id = movie_info.movie_id) And (movie_info.note like '%internet%')  And  (movie_info.info like 'USA:% 200%')