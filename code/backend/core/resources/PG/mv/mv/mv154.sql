create materialized view if not exists mv154 as select company_type.id AS company_type_id_0, company_type.kind AS kind, info_type.id AS info_type_id_2, info_type.info AS info_type_info_3, movie_companies.company_id AS company_id, movie_companies.movie_id AS movie_id, movie_info_idx.info AS movie_info_idx_info_6, title.production_year AS production_year, title.title AS title
 from movie_info_idx,movie_companies,company_type,title,info_type
 where (movie_companies.movie_id = movie_info_idx.movie_id) And (movie_info_idx.movie_id = title.id) And (movie_info_idx.info_type_id = info_type.id) And (movie_info_idx.info > '8.0') And (title.id = movie_companies.movie_id) And (movie_companies.company_type_id = company_type.id) And (company_type.kind = 'production companies') And (info_type.info = 'rating')