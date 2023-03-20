create materialized view if not exists mv151 as select company_type.id AS id, company_type.kind AS kind, movie_companies.company_id AS company_id, movie_companies.movie_id AS movie_id
 from company_type,movie_companies
 where (movie_companies.company_type_id = company_type.id) And (company_type.kind = 'production companies')