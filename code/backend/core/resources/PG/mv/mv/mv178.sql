create materialized view if not exists mv178 as select company_type.id AS company_type_id_0, company_type.kind AS kind, info_type.id AS info_type_id_2, info_type.info AS info_type_info_3, movie_companies.company_id AS company_id, movie_companies.movie_id AS movie_id, movie_info_idx.info AS movie_info_idx_info_6, movie_info_idx.info_type_id AS info_type_id, title.kind_id AS kind_id, title.title AS title
 from company_type,info_type,title,movie_companies,movie_info_idx,movie_info
 where (movie_companies.company_type_id = company_type.id) And (company_type.kind = 'production companies') And (movie_info.info_type_id = info_type.id) And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'rating') And (info_type.info = 'release dates') And (title.id = movie_companies.movie_id) And (movie_info.movie_id = title.id) And (movie_companies.movie_id = movie_info_idx.movie_id)