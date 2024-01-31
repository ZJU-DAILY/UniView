create materialized view if not exists mv148 as select keyword.id AS keyword_id_0, keyword.keyword AS keyword, link_type.id AS link_type_id_2, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_companies.movie_id AS movie_id, movie_companies.note AS note, title.production_year AS production_year, title.title AS title
 from link_type,movie_keyword,movie_companies,title,movie_link,keyword
 where (link_type.id = movie_link.link_type_id) And (title.id = movie_keyword.movie_id) And (movie_keyword.movie_id = movie_link.movie_id) And (movie_keyword.keyword_id = keyword.id) And (movie_companies.movie_id = movie_link.movie_id) And (movie_companies.note IS NOT NULL) And (movie_link.movie_id = title.id) And (keyword.keyword in ('sequel', 'revenge', 'based-on-novel'))