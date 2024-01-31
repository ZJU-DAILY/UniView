create materialized view if not exists mv89 as select aka_name.name AS aka_name_name_0, aka_name.person_id AS person_id, cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, name.gender AS gender, name.name AS name_name_7, role_type.role AS role
 from role_type,aka_name,cast_info,name
 where (cast_info.role_id = role_type.id) And (role_type.role = 'actress') And (aka_name.person_id = name.id) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) And (name.id = cast_info.person_id)