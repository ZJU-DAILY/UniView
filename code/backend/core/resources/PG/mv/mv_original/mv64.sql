select aka_name.name AS aka_name_name_0, aka_name.person_id AS person_id, cast_info.movie_id AS movie_id, info_type.info AS info_type_info_3, link_type.id AS id, link_type.link AS link, name.gender AS gender, name.name AS name_name_7, name.name_pcode_cf AS name_pcode_cf, person_info.info AS person_info_info_9, person_info.info_type_id AS info_type_id, person_info.note AS note, title.production_year AS production_year
 from link_type,person_info,name,title,movie_link,aka_name,cast_info,info_type
 where (movie_link.link_type_id = link_type.id) And (link_type.link in ('references', 'referenced in', 'features', 'featured in')) And (person_info.info_type_id = info_type.id) And (person_info.note IS NOT NULL) And (name.id = person_info.person_id) And (name.id = aka_name.person_id) And (cast_info.person_id = name.id) And (title.id = cast_info.movie_id) And (cast_info.movie_id = movie_link.linked_movie_id) And (aka_name.name IS NOT NULL)  And  (((aka_name.name like '%a%')) Or ((aka_name.name like 'A%'))) And (info_type.info = 'mini biography')