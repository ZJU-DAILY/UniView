create materialized view if not exists mv446 as select aka_name.person_id AS person_id, cast_info.movie_id AS cast_info_movie_id_1, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS char_name_name_5, complete_cast.movie_id AS complete_cast_movie_id_6, complete_cast.status_id AS status_id, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_9, info_type.info AS info_type_info_10, keyword.id AS keyword_id_11, keyword.keyword AS keyword, movie_info.info AS movie_info_info_13, name.gender AS gender, name.name AS name_name_15, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from aka_name,cast_info,title,info_type,name,movie_keyword,person_info,keyword,role_type,complete_cast,char_name,movie_info
 where (aka_name.person_id = cast_info.person_id) And (cast_info.person_id = name.id) And (person_info.person_id = cast_info.person_id) And (cast_info.movie_id = movie_info.movie_id) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) And (char_name.id = cast_info.person_role_id) And (role_type.id = cast_info.role_id) And (title.id = complete_cast.movie_id) And (title.id = movie_keyword.movie_id) And (movie_info.movie_id = title.id) And (info_type.id = movie_info.info_type_id) And (person_info.info_type_id = info_type.id) And (info_type.info = 'trivia') And (name.id = person_info.person_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'computer-animation') And (movie_info.info IS NOT NULL)  And  (((movie_info.info = '__LIKE__Japan:%200%')) Or ((movie_info.info = '__LIKE__USA:%200%')))