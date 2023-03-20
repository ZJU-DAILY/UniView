create materialized view if not exists mv418 as select aka_name.person_id AS person_id, cast_info.movie_id AS cast_info_movie_id_1, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS char_name_name_5, comp_cast_type.id AS comp_cast_type_id_6, comp_cast_type.kind AS kind, complete_cast.movie_id AS complete_cast_movie_id_8, complete_cast.status_id AS status_id, keyword.id AS keyword_id_10, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_info.info AS info, movie_info.info_type_id AS movie_info_info_type_id_14, movie_keyword.movie_id AS movie_keyword_movie_id_15, name.gender AS gender, name.name AS name_name_17, person_info.info_type_id AS person_info_info_type_id_18, title.production_year AS production_year, title.title AS title
 from aka_name,cast_info,title,name,comp_cast_type,movie_keyword,person_info,keyword,movie_companies,complete_cast,char_name,movie_info
 where (aka_name.person_id = cast_info.person_id) And (cast_info.person_id = name.id) And (person_info.person_id = cast_info.person_id) And (cast_info.movie_id = movie_info.movie_id) And (cast_info.movie_id = movie_keyword.movie_id) And (cast_info.note in ('(voice)', '(voice) (uncredited)', '(voice: English version)')) And (char_name.id = cast_info.person_role_id) And (title.id = complete_cast.movie_id) And (title.id = movie_keyword.movie_id) And (movie_companies.movie_id = title.id) And (movie_info.movie_id = title.id) And (name.id = person_info.person_id) And (comp_cast_type.id = complete_cast.subject_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'computer-animation') And (movie_info.info IS NOT NULL)  And  (((movie_info.info = '__LIKE__Japan:%200%')) Or ((movie_info.info = '__LIKE__USA:%200%')))