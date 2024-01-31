select company_type.kind AS kind, movie_companies.movie_id AS movie_id, movie_companies.note AS note
 from movie_companies,company_type
 where (movie_companies.company_type_id = company_type.id) And (movie_companies.note like '%(theatrical)%')  And  (movie_companies.note like '%(France)%') And (company_type.id = movie_companies.company_type_id) And (company_type.kind = 'production companies')