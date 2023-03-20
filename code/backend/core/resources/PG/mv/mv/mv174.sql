create materialized view if not exists mv174 as select company_name.country_code AS country_code, company_name.id AS company_name_id_1, company_type.id AS company_type_id_2, company_type.kind AS company_type_kind_3, info_type.id AS info_type_id_4, info_type.info AS info_type_info_5, kind_type.id AS kind_type_id_6, kind_type.kind AS kind_type_kind_7, movie_companies.movie_id AS movie_id, movie_info_idx.info AS movie_info_idx_info_9, title.title AS title
 from company_type,info_type,title,movie_companies,company_name,kind_type,movie_info_idx
 where (movie_companies.company_type_id = company_type.id) And (company_type.kind = 'production companies') And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'rating') And (title.id = movie_companies.movie_id) And (kind_type.id = title.kind_id) And (movie_companies.movie_id = movie_info_idx.movie_id) And (company_name.id = movie_companies.company_id)