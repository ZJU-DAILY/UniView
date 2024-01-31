select company_name.country_code AS country_code, company_name.id AS company_name_id_1, company_name.name AS name, company_type.id AS company_type_id_3, company_type.kind AS kind, keyword.id AS keyword_id_5, keyword.keyword AS keyword, link_type.id AS link_type_id_7, movie_companies.movie_id AS movie_id, movie_companies.note AS note, title.production_year AS production_year, title.title AS title
 from link_type,movie_keyword,movie_companies,title,company_type,company_name,movie_link,keyword
 where (link_type.id = movie_link.link_type_id) And (title.id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (movie_companies.movie_id = movie_link.movie_id) And (movie_companies.note IS NOT NULL) And (company_name.id = movie_companies.company_id) And (company_type.id = movie_companies.company_type_id) And (movie_link.movie_id = title.id) And (movie_keyword.movie_id = movie_link.movie_id) And (keyword.keyword in ('sequel', 'revenge', 'based-on-novel'))