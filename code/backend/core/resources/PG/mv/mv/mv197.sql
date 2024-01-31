create materialized view if not exists mv197 as select aka_title.movie_id AS movie_id, info_type.id AS id, info_type.info AS info_type_info_2, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_companies.note AS movie_companies_note_5, movie_info.info AS movie_info_info_6, movie_info.note AS movie_info_note_7, movie_keyword.keyword_id AS keyword_id, title.production_year AS production_year, title.title AS title
 from movie_info,movie_companies,title,aka_title,info_type,movie_keyword
 where (movie_info.info_type_id = info_type.id) And (movie_info.note like '%internet%')  And  (movie_info.info like 'USA:% 200%') And (title.id = movie_info.movie_id) And (title.id = movie_companies.movie_id) And (movie_companies.note like '%(200%)%')  And  (movie_companies.note like '%(worldwide)%') And (aka_title.movie_id = title.id) And (movie_keyword.movie_id = title.id) And (info_type.info = 'release dates')