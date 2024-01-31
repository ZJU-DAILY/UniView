select company_name.country_code AS country_code, company_name.id AS id, movie_companies.company_type_id AS company_type_id, movie_companies.movie_id AS movie_id
 from movie_companies,company_name
 where (movie_companies.company_id = company_name.id) And (company_name.country_code = '[ru]')