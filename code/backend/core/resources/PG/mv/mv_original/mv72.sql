select aka_name.name AS name, aka_name.person_id AS person_id, cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.role_id AS role_id, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from role_type,title,aka_name,cast_info
 where (cast_info.role_id = role_type.id) And (role_type.role = 'actress') And (title.id = cast_info.movie_id) And (aka_name.person_id = cast_info.person_id) And (cast_info.note = '(voice: English version)')