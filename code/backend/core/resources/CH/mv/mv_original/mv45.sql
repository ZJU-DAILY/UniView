select movie_info.info AS movie_info_info_0, movie_info.movie_id AS movie_info_movie_id_1, movie_info_idx.info_type_id AS movie_info_idx_info_type_id_2, company_name.name AS name, company_type.id AS company_type_id_4, company_type.kind AS kind, movie_info_idx.movie_id AS movie_info_idx_movie_id_6, movie_info.info_type_id AS movie_info_info_type_id_7, company_name.id AS company_name_id_8, title.id AS title_id_9, movie_info_idx.info AS movie_info_idx_info_10, company_name.country_code AS country_code, info_type.info AS info_type_info_12, title.production_year AS production_year, title.title AS title, movie_companies.movie_id AS movie_companies_movie_id_15
 from title,movie_info_idx,info_type,company_type,company_name,movie_info,movie_companies
 where title.production_year >= 2000  And  title.production_year <= 2010 And title.id = movie_info.movie_id  And  title.id = movie_info_idx.movie_id  And  title.id = movie_companies.movie_id And (movie_info_idx.info > '7.0') And  movie_companies.movie_id = movie_info_idx.movie_id  And  movie_info.movie_id = movie_info_idx.movie_id And (movie_info_idx.info_type_id = info_type.id) And movie_info.info_type_id = info_type.id  And (company_type.kind = 'production companies') And (company_type.id = movie_companies.company_type_id) And (company_name.country_code = '[us]') And (company_name.id = movie_companies.company_id) And ((movie_info.info = 'Drama') Or (movie_info.info =  'Horror') Or (movie_info.info =  'Western') Or (movie_info.info =  'Family')) And  movie_companies.movie_id = movie_info.movie_id 