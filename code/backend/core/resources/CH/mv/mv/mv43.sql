create materialized view if not exists mv43 engine = MergeTree() order by tuple() POPULATE as 
select movie_info.info AS movie_info_info_0, company_name.country_code AS country_code, info_type.info AS info_type_info_2, company_name.id AS company_name_id_3, movie_info.movie_id AS movie_id, movie_companies.company_type_id AS company_type_id, info_type.id AS info_type_id_6, company_name.name AS name, company_type.kind AS kind
 from info_type,company_type,company_name,movie_info,movie_companies
 where (movie_info.info_type_id = info_type.id) And (company_type.id = movie_companies.company_type_id) And (company_type.kind = 'production companies') And (company_name.id = movie_companies.company_id) And (company_name.country_code = '[us]') And (movie_companies.movie_id = movie_info.movie_id) And ((movie_info.info = 'Drama') Or (movie_info.info =  'Horror') Or (movie_info.info =  'Western') Or (movie_info.info =  'Family'))