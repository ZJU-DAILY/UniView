create materialized view if not exists mv10 engine = MergeTree() order by tuple() POPULATE as 
select company_name.country_code AS country_code, movie_companies.movie_id AS movie_id, company_name.id AS id
 from company_name,movie_companies
 where (company_name.id = movie_companies.company_id) And (company_name.country_code = '[sm]')