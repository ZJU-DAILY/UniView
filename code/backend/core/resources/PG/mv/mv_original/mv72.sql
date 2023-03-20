select aka_name.name AS name, aka_name.person_id AS person_id, cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.role_id AS role_id, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from aka_name,cast_info,title,role_type
 where (aka_name.person_id = cast_info.person_id) And (cast_info.role_id = role_type.id) And (cast_info.note = '(voice: English version)') And (title.id = cast_info.movie_id) And (role_type.role = 'actress')