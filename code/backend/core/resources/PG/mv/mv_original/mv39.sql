select company_type.id AS id, company_type.kind AS kind, movie_companies.movie_id AS movie_id, movie_companies.note AS note
 from movie_companies,company_type
 where (movie_companies.company_type_id = company_type.id) And (movie_companies.note not like '%(TV)%')  And  (movie_companies.note like '%(USA)%') And (company_type.kind = 'production companies')