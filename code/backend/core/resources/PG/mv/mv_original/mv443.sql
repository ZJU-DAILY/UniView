select aka_name.person_id AS aka_name_person_id_0, cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS name, info_type.id AS info_type_id_6, info_type.info AS info_type_info_7, keyword.id AS keyword_id_8, keyword.keyword AS keyword, movie_info.info AS movie_info_info_10, movie_info.info_type_id AS info_type_id, person_info.person_id AS person_info_person_id_12, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from char_name,person_info,role_type,movie_info,keyword,title,aka_name,cast_info,info_type,movie_keyword
 where (char_name.id = cast_info.person_role_id) And (person_info.info_type_id = info_type.id) And (person_info.person_id = cast_info.person_id) And (role_type.id = cast_info.role_id) And (cast_info.movie_id = movie_info.movie_id) And (movie_info.movie_id = title.id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_info.info IS NOT NULL)  And  (((movie_info.info = '__LIKE__Japan:%200%')) Or ((movie_info.info = '__LIKE__USA:%200%'))) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'computer-animation') And (title.id = movie_keyword.movie_id) And (aka_name.person_id = cast_info.person_id) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) And (info_type.info = 'trivia')