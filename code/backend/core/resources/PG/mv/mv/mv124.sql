create materialized view if not exists mv124 as select cast_info.movie_id AS cast_info_movie_id_0, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS name, company_name.country_code AS country_code, company_name.id AS id, movie_companies.movie_id AS movie_companies_movie_id_7, title.production_year AS production_year, title.title AS title
 from company_type,cast_info,title,movie_companies,role_type,company_name,char_name
 where (company_type.id = movie_companies.company_type_id) And (cast_info.movie_id = title.id) And (cast_info.person_role_id = char_name.id) And (cast_info.role_id = role_type.id) And (cast_info.note like '%(producer)%') And (movie_companies.movie_id = title.id) And (title.production_year > 1990) And (movie_companies.company_type_id = company_type.id) And (movie_companies.company_id = company_name.id) And (company_name.country_code = '[us]')