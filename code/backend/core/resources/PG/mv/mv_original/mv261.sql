select company_name.country_code AS country_code, company_name.id AS company_name_id_1, company_name.name AS name, company_type.id AS company_type_id_3, company_type.kind AS kind, keyword.id AS keyword_id_5, keyword.keyword AS keyword, link_type.id AS link_type_id_7, link_type.link AS link, movie_companies.movie_id AS movie_id, movie_companies.note AS note, movie_info.info AS info, title.production_year AS production_year, title.title AS title
 from link_type,movie_info,keyword,movie_companies,company_type,title,company_name,movie_link,movie_keyword
 where (movie_link.link_type_id = link_type.id) And (link_type.link like '%follow%') And (movie_link.movie_id = movie_info.movie_id) And (movie_info.info in ('Germany', 'German')) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'sequel') And (movie_companies.movie_id = movie_link.movie_id) And (movie_companies.company_type_id = company_type.id) And (movie_companies.note IS NULL) And (company_name.id = movie_companies.company_id) And (company_type.kind = 'production companies') And (title.id = movie_keyword.movie_id) And (movie_link.movie_id = title.id) And (movie_keyword.movie_id = movie_link.movie_id)