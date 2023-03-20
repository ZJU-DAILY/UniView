select aka_name.person_id AS person_id, cast_info.movie_id AS cast_info_movie_id_1, cast_info.note AS note, cast_info.person_role_id AS person_role_id, cast_info.role_id AS role_id, char_name.name AS char_name_name_5, comp_cast_type.id AS comp_cast_type_id_6, comp_cast_type.kind AS kind, company_name.country_code AS country_code, company_name.id AS company_name_id_9, complete_cast.movie_id AS complete_cast_movie_id_10, complete_cast.subject_id AS subject_id, info_type.id AS info_type_id_12, info_type.info AS info_type_info_13, keyword.id AS keyword_id_14, keyword.keyword AS keyword, movie_info.info AS movie_info_info_16, name.gender AS gender, name.name AS name_name_18, person_info.info_type_id AS info_type_id, role_type.role AS role, title.production_year AS production_year, title.title AS title
 from aka_name,cast_info,title,name,comp_cast_type,movie_keyword,info_type,person_info,keyword,movie_companies,role_type,company_name,complete_cast,char_name,movie_info
 where (aka_name.person_id = cast_info.person_id) And (cast_info.person_id = name.id) And (person_info.person_id = cast_info.person_id) And (cast_info.movie_id = movie_info.movie_id) And (cast_info.note in ('(voice)', '(voice) (uncredited)', '(voice: English version)')) And (char_name.id = cast_info.person_role_id) And (role_type.id = cast_info.role_id) And (title.id = complete_cast.movie_id) And (title.id = movie_keyword.movie_id) And (movie_companies.movie_id = title.id) And (movie_info.movie_id = title.id) And (name.id = person_info.person_id) And (comp_cast_type.id = complete_cast.status_id) And (comp_cast_type.id = complete_cast.subject_id) And (cast_info.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (info_type.id = movie_info.info_type_id) And (info_type.id = person_info.info_type_id) And (keyword.keyword = 'computer-animation') And (company_name.id = movie_companies.company_id) And (movie_info.info IS NOT NULL)  And  (((movie_info.info = '__LIKE__Japan:%200%')) Or ((movie_info.info = '__LIKE__USA:%200%')))