select aka_title.movie_id AS movie_id, company_name.country_code AS country_code, company_name.id AS company_name_id_2, info_type.id AS info_type_id_3, info_type.info AS info_type_info_4, movie_companies.company_type_id AS company_type_id, movie_info.info AS movie_info_info_6, movie_info.note AS note
 from info_type,movie_companies,company_name,aka_title,movie_info
 where (info_type.id = movie_info.info_type_id) And (info_type.info = 'release dates') And (movie_companies.movie_id = aka_title.movie_id) And (company_name.id = movie_companies.company_id) And (aka_title.movie_id = movie_info.movie_id) And (movie_info.info IS NOT NULL)  And  (movie_info.note like '%internet%')  And  (((movie_info.info like 'USA:% 199%')) Or ((movie_info.info like 'USA:% 200%')))