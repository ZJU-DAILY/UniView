select aka_name.name AS aka_name_name_0, aka_name.person_id AS person_id, cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.role_id AS role_id, char_name.id AS id, char_name.name AS char_name_name_6, name.gender AS gender, name.name AS name_name_8, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from aka_name,cast_info,title,name,role_type,char_name
 where (aka_name.person_id = name.id) And (cast_info.role_id = role_type.id) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) And (name.id = cast_info.person_id) And (char_name.id = cast_info.person_role_id) And (title.id = cast_info.movie_id) And (role_type.role = 'actress')