create materialized view if not exists mv129 as select company_type.id AS company_type_id_0, company_type.kind AS kind, keyword.id AS keyword_id_2, keyword.keyword AS keyword, link_type.id AS link_type_id_4, link_type.link AS link, movie_companies.company_id AS company_id, movie_companies.movie_id AS movie_id, movie_companies.note AS note
 from link_type,company_type,movie_keyword,keyword,movie_companies,movie_link
 where (movie_link.link_type_id = link_type.id) And (link_type.link like '%follow%') And (movie_companies.company_type_id = company_type.id) And (company_type.kind = 'production companies') And (movie_keyword.movie_id = movie_link.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'sequel') And (movie_companies.movie_id = movie_link.movie_id) And (movie_companies.note IS NULL)