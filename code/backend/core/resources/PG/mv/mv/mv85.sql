create materialized view if not exists mv85 as select cast_info.movie_id AS movie_id, cast_info.person_id AS person_id, cast_info.role_id AS role_id, company_name.country_code AS country_code, company_name.id AS id, role_type.role AS role, title.title AS title
 from name,role_type,movie_companies,title,company_name,cast_info
 where (name.id = cast_info.person_id) And (cast_info.role_id = role_type.id) And (role_type.role = 'costume designer') And (cast_info.movie_id = movie_companies.movie_id) And (movie_companies.company_id = company_name.id) And (title.id = cast_info.movie_id) And (company_name.country_code = '[us]')