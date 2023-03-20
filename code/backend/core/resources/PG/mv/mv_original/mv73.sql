select aka_name.name AS aka_name_name_0, cast_info.note AS note, cast_info.person_id AS person_id, cast_info.role_id AS role_id, name.name AS name_name_4, role_type.role AS role, title.id AS id, title.production_year AS production_year, title.title AS title
 from aka_name,cast_info,title,name,role_type
 where (aka_name.person_id = name.id) And (aka_name.person_id = cast_info.person_id) And (cast_info.role_id = role_type.id) And (cast_info.note = '(voice: English version)') And (title.id = cast_info.movie_id) And (name.id = aka_name.person_id) And (role_type.role = 'actress')