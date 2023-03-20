create materialized view if not exists mv51 engine = MergeTree() order by tuple() POPULATE as 
select movie_info_idx.info AS movie_info_idx_info_0, company_name.country_code AS country_code, info_type.info AS info_type_info_2, company_name.id AS company_name_id_3, movie_info.movie_id AS movie_info_movie_id_4, movie_companies.company_type_id AS company_type_id, movie_info_idx.movie_id AS movie_info_idx_movie_id_6, info_type.id AS info_type_id_7, company_name.name AS name, movie_info.info_type_id AS info_type_id, movie_companies.movie_id AS movie_companies_movie_id_10, company_type.kind AS kind
 from movie_info_idx,info_type,company_type,company_name,movie_info,movie_companies
 where movie_companies.movie_id = movie_info_idx.movie_id  And  movie_info.movie_id = movie_info_idx.movie_id And (movie_info_idx.info_type_id = info_type.id) And (movie_info_idx.info = 'release dates') And info_type.id = movie_info_idx.info_type_id  And  info_type.id = movie_info.info_type_id And info_type.info = 'release dates'  And  info_type.info = 'rating' And (company_type.id = movie_companies.company_type_id) And (company_type.kind = 'production companies') And (company_name.id = movie_companies.company_id) And (company_name.country_code = '[us]') And  movie_info.movie_id = movie_companies.movie_id And  movie_info_idx.movie_id = movie_companies.movie_id