create materialized view if not exists mv35 engine = MergeTree() order by tuple() POPULATE as 
select cast_info.note AS note, cast_info.movie_id AS movie_id, char_name.name AS name, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id
 from cast_info,char_name
 where (char_name.id = cast_info.person_role_id) And ((cast_info.note like '%(producer)%'))