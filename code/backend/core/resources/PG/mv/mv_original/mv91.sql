select aka_name.name AS aka_name_name_0, aka_name.person_id AS person_id, cast_info.movie_id AS movie_id, cast_info.note AS cast_info_note_3, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, company_name.country_code AS country_code, company_name.id AS id, movie_companies.note AS movie_companies_note_8, name.gender AS gender, name.name AS name_name_10, role_type.role AS role
 from name,role_type,movie_companies,company_name,aka_name,cast_info
 where (name.id = cast_info.person_id) And (aka_name.person_id = name.id) And (cast_info.role_id = role_type.id) And (role_type.role = 'actress') And (movie_companies.movie_id = cast_info.movie_id) And (movie_companies.note IS NOT NULL)  And  (((movie_companies.note like '%(USA)%')) Or ((movie_companies.note like '%(worldwide)%'))) And (company_name.id = movie_companies.company_id) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)'))