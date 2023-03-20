select aka_title.movie_id AS aka_title_movie_id_0, aka_title.title AS aka_title_title_1, company_name.country_code AS country_code, company_name.id AS company_name_id_3, info_type.id AS info_type_id_4, info_type.info AS info, movie_companies.company_type_id AS company_type_id, movie_info.movie_id AS movie_info_movie_id_7, movie_info.note AS note, title.production_year AS production_year, title.title AS title_title_10
 from info_type,title,movie_companies,company_name,aka_title,movie_info
 where (info_type.id = movie_info.info_type_id) And (info_type.info = 'release dates') And (title.id = movie_companies.movie_id) And (aka_title.movie_id = title.id) And (movie_companies.movie_id = title.id) And (company_name.id = movie_companies.company_id) And (movie_info.note like '%internet%') And (title.id = movie_info.movie_id)