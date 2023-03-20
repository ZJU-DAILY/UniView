select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS comp_cast_type_kind_1, company_name.country_code AS country_code, company_name.id AS company_name_id_3, company_name.name AS name, company_type.id AS company_type_id_5, complete_cast.movie_id AS complete_cast_movie_id_6, info_type.id AS info_type_id_7, info_type.info AS info_type_info_8, keyword.id AS keyword_id_9, keyword.keyword AS keyword, kind_type.id AS kind_type_id_11, kind_type.kind AS kind_type_kind_12, movie_companies.movie_id AS movie_companies_movie_id_13, movie_companies.note AS note, movie_info.info AS movie_info_info_15, movie_info_idx.info AS movie_info_idx_info_16, movie_info_idx.info_type_id AS info_type_id, title.production_year AS production_year, title.title AS title
 from company_type,info_type,title,comp_cast_type,movie_keyword,keyword,movie_companies,kind_type,company_name,complete_cast,movie_info_idx,movie_info
 where (company_type.id = movie_companies.company_type_id) And (info_type.id = movie_info.info_type_id) And (title.id = movie_info.movie_id) And (title.id = movie_keyword.movie_id) And (movie_info_idx.movie_id = title.id) And (kind_type.id = title.kind_id) And (comp_cast_type.id = complete_cast.subject_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = 'complete') And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence')) And (title.id = movie_companies.movie_id) And (movie_companies.note not like '%(USA)%')  And  (movie_companies.note like '%(200%)%') And (company_name.id = movie_companies.company_id) And (movie_info_idx.info < '8.5') And (info_type.id = movie_info_idx.info_type_id) And (movie_info.movie_id = title.id) And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American'))