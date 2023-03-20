select aka_title.movie_id AS movie_id, info_type.id AS id, info_type.info AS info_type_info_2, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_info.info AS movie_info_info_5, movie_info.note AS note
 from info_type,movie_info,aka_title,movie_companies
 where (info_type.id = movie_info.info_type_id) And (info_type.info = 'release dates') And (aka_title.movie_id = movie_info.movie_id) And (movie_info.info IS NOT NULL)  And  (movie_info.note like '%internet%')  And  (((movie_info.info like 'USA:% 199%')) Or ((movie_info.info like 'USA:% 200%'))) And (movie_companies.movie_id = aka_title.movie_id)