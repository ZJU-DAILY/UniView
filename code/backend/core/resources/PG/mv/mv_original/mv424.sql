select aka_name.person_id AS person_id, cast_info.movie_id AS cast_info_movie_id_1, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS name, keyword.id AS id, keyword.keyword AS keyword, movie_info.info AS info, movie_info.info_type_id AS info_type_id, movie_keyword.movie_id AS movie_keyword_movie_id_10
 from aka_name,cast_info,movie_keyword,keyword,char_name,movie_info
 where (aka_name.person_id = cast_info.person_id) And (cast_info.movie_id = movie_info.movie_id) And (cast_info.note in ('(voice)', '(voice) (uncredited)', '(voice: English version)')) And (char_name.id = cast_info.person_role_id) And (cast_info.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'computer-animation') And (movie_info.info = '__LIKE__USA:%200%')