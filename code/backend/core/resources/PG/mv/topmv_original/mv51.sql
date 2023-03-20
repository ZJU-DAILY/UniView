select info_type.id AS id, info_type.info AS info, person_info.note AS note, person_info.person_id AS person_id
 from person_info,info_type
 where (info_type.id = person_info.info_type_id) And (person_info.note = 'Volker Boehm') And (info_type.info = 'mini biography')