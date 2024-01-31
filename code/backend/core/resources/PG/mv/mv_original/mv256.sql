select aka_name.person_id AS person_id, cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.role_id AS role_id, char_name.id AS char_name_id_4, info_type.id AS info_type_id_5, info_type.info AS info, movie_companies.company_id AS company_id, movie_info.info_type_id AS info_type_id, name.gender AS gender, name.name AS name, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from char_name,name,role_type,movie_info,movie_companies,title,aka_name,cast_info,info_type
 where (char_name.id = cast_info.person_role_id) And (name.id = cast_info.person_id) And (aka_name.person_id = name.id) And (cast_info.role_id = role_type.id) And (role_type.role = 'actress') And (movie_info.movie_id = title.id) And (movie_info.info_type_id = info_type.id) And (movie_companies.movie_id = title.id) And (title.id = cast_info.movie_id) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) And (info_type.info = 'release dates')