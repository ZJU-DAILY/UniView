select cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id
 from cast_info,role_type
 where (cast_info.role_id = role_type.id) And (cast_info.note like '%(producer)%')