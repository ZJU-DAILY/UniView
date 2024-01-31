select aka_name.person_id AS person_id, cast_info.movie_id AS cast_info_movie_id_1, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS name, keyword.id AS id, keyword.keyword AS keyword, movie_info.info AS info, movie_info.info_type_id AS info_type_id, movie_keyword.movie_id AS movie_keyword_movie_id_10, title.production_year AS production_year, title.title AS title
 from movie_keyword,char_name,movie_info,title,aka_name,cast_info,keyword
 where (title.id = movie_keyword.movie_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (char_name.id = cast_info.person_role_id) And (cast_info.movie_id = movie_info.movie_id) And (movie_info.movie_id = title.id) And (movie_info.info IS NOT NULL)  And  (((movie_info.info = '__LIKE__Japan:%200%')) Or ((movie_info.info = '__LIKE__USA:%200%'))) And (aka_name.person_id = cast_info.person_id) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) And (keyword.keyword = 'computer-animation')