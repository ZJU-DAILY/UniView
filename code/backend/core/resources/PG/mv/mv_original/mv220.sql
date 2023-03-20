select aka_title.movie_id AS aka_title_movie_id_0, aka_title.title AS aka_title_title_1, info_type.id AS id, info_type.info AS info, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_info.movie_id AS movie_info_movie_id_6, movie_info.note AS note, title.production_year AS production_year, title.title AS title_title_9
 from info_type,title,movie_companies,aka_title,movie_info
 where (info_type.id = movie_info.info_type_id) And (info_type.info = 'release dates') And (title.id = movie_info.movie_id) And (aka_title.movie_id = title.id) And (movie_companies.movie_id = title.id) And (movie_info.note like '%internet%')