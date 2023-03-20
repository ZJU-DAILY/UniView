create materialized view if not exists mv437 as select cast_info.movie_id AS movie_id, cast_info.note AS note, cast_info.person_id AS person_id, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, keyword.id AS id, keyword.keyword AS keyword, movie_info.info AS info, movie_info.info_type_id AS info_type_id
 from keyword,movie_info,cast_info,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'computer-animation') And (cast_info.movie_id = movie_info.movie_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_info.info IS NOT NULL)  And  (((movie_info.info = '__LIKE__Japan:%200%')) Or ((movie_info.info = '__LIKE__USA:%200%'))) And (cast_info.note in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)'))