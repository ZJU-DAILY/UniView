create materialized view if not exists mv136 as select company_name.country_code AS country_code, company_name.id AS company_name_id_1, company_name.name AS name, company_type.id AS company_type_id_3, company_type.kind AS kind, link_type.id AS link_type_id_5, link_type.link AS link, movie_companies.movie_id AS movie_companies_movie_id_7, movie_companies.note AS note, movie_keyword.keyword_id AS keyword_id, movie_keyword.movie_id AS movie_keyword_movie_id_10, title.production_year AS production_year, title.title AS title
 from link_type,movie_companies,title,company_type,company_name,movie_link,movie_keyword
 where (movie_link.link_type_id = link_type.id) And (link_type.link like '%follows%') And (movie_companies.movie_id = movie_link.movie_id) And (movie_companies.company_type_id = company_type.id) And (movie_companies.note IS NULL) And (company_name.id = movie_companies.company_id) And (title.id = movie_link.movie_id) And (movie_keyword.movie_id = title.id) And (title.title like '%Money%')  And  (title.production_year = 1998) And (company_type.kind = 'production companies')