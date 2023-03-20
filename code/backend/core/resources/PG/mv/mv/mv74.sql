create materialized view if not exists mv74 as select company_name.country_code AS country_code, company_name.id AS id, movie_companies.movie_id AS movie_id, movie_companies.note AS note
 from company_name,movie_companies
 where (company_name.id = movie_companies.company_id) And (movie_companies.note like '%(Japan)%')  And  (movie_companies.note not like '%(USA)%')  And  (((movie_companies.note like '%(2006)%')) Or ((movie_companies.note like '%(2007)%')))