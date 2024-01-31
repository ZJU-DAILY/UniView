select cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS name, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from role_type,title,char_name,cast_info
 where (cast_info.role_id = role_type.id) And (role_type.role = 'actor') And (title.id = cast_info.movie_id) And (char_name.id = cast_info.person_role_id) And (cast_info.note like '%(voice)%')  And  (cast_info.note like '%(uncredited)%')