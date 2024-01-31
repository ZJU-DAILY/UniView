select company_type.id AS company_type_id_0, company_type.kind AS kind, info_type.id AS info_type_id_2, movie_companies.movie_id AS movie_id, movie_companies.note AS note, movie_info.info AS info
 from movie_companies,company_type,movie_info,info_type
 where (movie_info.movie_id = movie_companies.movie_id) And (company_type.id = movie_companies.company_type_id) And (movie_companies.note like '%(VHS)%')  And  (movie_companies.note like '%(USA)%')  And  (movie_companies.note like '%(1994)%') And (company_type.kind = 'production companies') And (movie_info.info in ('USA', 'America')) And (info_type.id = movie_info.info_type_id)