create materialized view if not exists mv76 as select cast_info.movie_id AS movie_id, cast_info.person_id AS person_id, cast_info.role_id AS role_id, role_type.role AS role
 from role_type,cast_info
 where (cast_info.role_id = role_type.id) And (role_type.role = 'writer')