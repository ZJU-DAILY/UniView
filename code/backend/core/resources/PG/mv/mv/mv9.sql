create materialized view if not exists mv9 as select info_type.id AS id, info_type.info AS info, movie_companies.company_type_id AS company_type_id, movie_companies.movie_id AS movie_id, movie_companies.note AS note
 from movie_companies,info_type,movie_info_idx
 where (movie_companies.movie_id = movie_info_idx.movie_id) And (movie_companies.note not like '%(as Metro-Goldwyn-Mayer Pictures)%')  And  (movie_companies.note like '%(co-production)%') And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'top 250 rank')