create materialized view if not exists mv3 engine = MergeTree() order by tuple() POPULATE as 
select title.id AS title_id_0, movie_companies.movie_id AS movie_companies_movie_id_1, info_type.info AS info, movie_companies.company_type_id AS company_type_id, movie_info_idx.movie_id AS movie_info_idx_movie_id_4, info_type.id AS info_type_id_5, title.production_year AS production_year, title.title AS title, movie_companies.note AS note, company_type.kind AS kind
 from title,movie_info_idx,info_type,company_type,movie_companies
 where title.id = movie_companies.movie_id  And  title.id = movie_info_idx.movie_id And  movie_companies.movie_id = movie_info_idx.movie_id And (info_type.id = movie_info_idx.info_type_id) And (info_type.info = 'top 250 rank') And (company_type.kind = 'production companies') And (company_type.id = movie_companies.company_type_id) And Not (((movie_companies.note like '%(as Metro-Goldwyn-Mayer Pictures)%')))  And  (((movie_companies.note like '%(co-production)%')) Or ((movie_companies.note like '%(presents)%')))