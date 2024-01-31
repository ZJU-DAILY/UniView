select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS comp_cast_type_kind_1, company_name.country_code AS country_code, company_name.id AS company_name_id_3, company_name.name AS name, company_type.id AS company_type_id_5, company_type.kind AS company_type_kind_6, complete_cast.movie_id AS complete_cast_movie_id_7, keyword.id AS keyword_id_8, keyword.keyword AS keyword, link_type.id AS link_type_id_10, link_type.link AS link, movie_companies.note AS note, movie_info.info AS info, movie_keyword.movie_id AS movie_keyword_movie_id_14, movie_link.movie_id AS movie_link_movie_id_15, title.production_year AS production_year, title.title AS title
 from link_type,movie_info,keyword,movie_companies,company_type,title,company_name,comp_cast_type,complete_cast,movie_link,movie_keyword
 where (link_type.id = movie_link.link_type_id) And (movie_info.movie_id = movie_companies.movie_id) And (movie_info.info in ('Sweden', 'Germany', 'Swedish', 'German')) And (keyword.id = movie_keyword.keyword_id) And (movie_companies.movie_id = complete_cast.movie_id) And (movie_companies.movie_id = movie_keyword.movie_id) And (movie_companies.company_type_id = company_type.id) And (movie_companies.note IS NULL) And (company_name.id = movie_companies.company_id) And (company_type.kind = 'production companies') And (title.id = movie_keyword.movie_id) And (title.id = movie_link.movie_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = 'complete') And (comp_cast_type.id = complete_cast.subject_id)