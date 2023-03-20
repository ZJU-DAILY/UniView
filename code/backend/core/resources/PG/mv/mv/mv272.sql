create materialized view if not exists mv272 as select company_name.country_code AS country_code, company_name.id AS company_name_id_1, company_name.name AS name, info_type.id AS info_type_id_3, info_type.info AS info_type_info_4, keyword.id AS keyword_id_5, keyword.keyword AS keyword, kind_type.id AS kind_type_id_7, kind_type.kind AS kind, movie_companies.company_type_id AS company_type_id, movie_companies.movie_id AS movie_companies_movie_id_10, movie_companies.note AS note, movie_info.info AS movie_info_info_12, movie_info_idx.info AS movie_info_idx_info_13, movie_info_idx.info_type_id AS info_type_id, movie_keyword.movie_id AS movie_keyword_movie_id_15, title.production_year AS production_year, title.title AS title
 from info_type,title,movie_keyword,keyword,movie_companies,kind_type,company_name,movie_info_idx,movie_info
 where (movie_info.info_type_id = info_type.id) And (movie_info_idx.info_type_id = info_type.id) And (info_type.info = 'countries') And (info_type.info = 'rating') And (title.id = movie_companies.movie_id) And (title.id = movie_keyword.movie_id) And (movie_info.movie_id = title.id) And (movie_info_idx.movie_id = title.id) And (kind_type.id = title.kind_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence')) And (movie_companies.note not like '%(USA)%')  And  (movie_companies.note like '%(200%)%') And (company_name.id = movie_companies.company_id) And (movie_info_idx.info < '7.0') And (movie_info.info in ('Germany', 'German', 'USA', 'American'))