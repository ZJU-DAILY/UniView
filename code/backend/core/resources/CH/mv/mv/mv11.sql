create materialized view if not exists mv11 engine = MergeTree() order by tuple() POPULATE as 
select movie_keyword.movie_id AS movie_id, company_name.country_code AS country_code, keyword.id AS keyword_id_2, company_name.id AS company_name_id_3, keyword.keyword AS keyword
 from company_name,keyword,movie_keyword,movie_companies
 where (company_name.id = movie_companies.company_id) And (company_name.country_code = '[sm]') And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'character-name-in-title') And (movie_companies.movie_id = movie_keyword.movie_id)