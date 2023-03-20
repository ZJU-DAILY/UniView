select movie_companies.company_type_id AS company_type_id, movie_companies.note AS note, movie_companies.movie_id AS movie_id, company_type.kind AS kind
 from company_type,movie_companies
 where (company_type.id = movie_companies.company_type_id) And (company_type.kind = 'production companies') And Not (((movie_companies.note like '%(as Metro-Goldwyn-Mayer Pictures)%')))