create materialized view if not exists mv3 as select company_type.id AS company_type_id_0, company_type.kind AS kind, info_type.id AS info_type_id_2, info_type.info AS info, movie_companies.movie_id AS movie_id, movie_companies.note AS note
 from company_type,info_type,movie_info_idx,movie_companies
 where (movie_companies.company_type_id = company_type.id) And (company_type.kind = 'production companies') And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'top 250 rank') And (movie_companies.movie_id = movie_info_idx.movie_id) And (movie_companies.note not like '%(as Metro-Goldwyn-Mayer Pictures)%')  And  (((movie_companies.note like '%(co-production)%')) Or ((movie_companies.note like '%(presents)%')))