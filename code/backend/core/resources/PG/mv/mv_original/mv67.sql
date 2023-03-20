select cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.person_id AS person_id, cast_info.role_id AS role_id, name.name AS name, role_type.role AS role
 from cast_info,name,role_type
 where (cast_info.role_id = role_type.id) And (cast_info.note = '(voice: English version)') And (name.id = cast_info.person_id) And (role_type.role = 'actress')