create materialized view if not exists mv58 as select info_type.info AS info_type_info_0, person_info.info AS person_info_info_1, person_info.note AS note, person_info.person_id AS person_id
 from person_info,info_type
 where (info_type.id = person_info.info_type_id) And (person_info.note IS NOT NULL) And (person_info.info_type_id = info_type.id) And (info_type.info = 'mini biography')