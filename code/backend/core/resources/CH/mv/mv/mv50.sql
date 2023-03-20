create materialized view if not exists mv50 engine = MergeTree() order by tuple() POPULATE as 
select company_name.country_code AS country_code, company_name.id AS id, info_type.info AS info, movie_info.movie_id AS movie_id, movie_companies.company_type_id AS company_type_id, company_name.name AS name, company_type.kind AS kind
 from info_type,company_type,company_name,movie_info,movie_companies
 where (movie_info.info_type_id = info_type.id) And info_type.info = 'release dates'  And  info_type.info = 'rating' And (company_type.id = movie_companies.company_type_id) And (company_type.kind = 'production companies') And (company_name.id = movie_companies.company_id) And (company_name.country_code = '[us]') And (movie_info.movie_id = movie_companies.movie_id) And (info_type.id = movie_info.info_type_id)