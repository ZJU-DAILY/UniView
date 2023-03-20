create materialized view if not exists mv82 as select cast_info.movie_id AS movie_id, cast_info.person_id AS person_id, cast_info.role_id AS role_id, role_type.role AS role
 from cast_info,role_type
 where (cast_info.role_id = role_type.id) And (role_type.role = 'costume designer')