select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS comp_cast_type_kind_1, company_type.id AS company_type_id_2, company_type.kind AS company_type_kind_3, complete_cast.movie_id AS complete_cast_movie_id_4, complete_cast.subject_id AS subject_id, keyword.id AS keyword_id_6, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.movie_id AS movie_companies_movie_id_9, movie_companies.note AS note, movie_info.info AS info, movie_link.link_type_id AS link_type_id, title.production_year AS production_year, title.title AS title
 from company_type,title,comp_cast_type,movie_keyword,keyword,movie_companies,complete_cast,movie_info,movie_link
 where (company_type.id = movie_companies.company_type_id) And (title.id = movie_keyword.movie_id) And (movie_link.movie_id = title.id) And (comp_cast_type.id = complete_cast.status_id) And (comp_cast_type.id = complete_cast.subject_id) And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_keyword.movie_id = movie_link.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'sequel') And (movie_companies.movie_id = movie_link.movie_id) And (movie_companies.note IS NULL) And (movie_info.info in ('Sweden', 'Germany', 'Swedish', 'German'))