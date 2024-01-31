create materialized view if not exists mv124 as select cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.role_id AS role_id, char_name.name AS name, company_name.country_code AS country_code, company_name.id AS company_name_id_5, company_type.id AS company_type_id_6, role_type.id AS role_type_id_7, title.production_year AS production_year, title.title AS title
 from char_name,role_type,movie_companies,title,company_type,company_name,cast_info
 where (char_name.id = cast_info.person_role_id) And (cast_info.role_id = role_type.id) And (movie_companies.company_type_id = company_type.id) And (movie_companies.movie_id = title.id) And (movie_companies.company_id = company_name.id) And (cast_info.movie_id = title.id) And (title.production_year > 1990) And (company_name.country_code = '[us]') And (cast_info.person_role_id = char_name.id) And (cast_info.note like '%(producer)%')