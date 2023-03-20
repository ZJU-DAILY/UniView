create materialized view if not exists mv46 engine = MergeTree() order by tuple() POPULATE as 
select company_name.country_code AS country_code, company_name.id AS id, movie_companies.company_type_id AS company_type_id, movie_companies.movie_id AS movie_id, company_type.kind AS kind
 from company_type,company_name,movie_companies
 where 
 (company_type.id = movie_companies.company_type_id) 
 And (company_type.kind = 'production companies') 
 And (company_name.id = movie_companies.company_id) 
 And (company_name.country_code = '[de]')