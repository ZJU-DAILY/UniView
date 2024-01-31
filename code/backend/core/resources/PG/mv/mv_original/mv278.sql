select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, complete_cast.movie_id AS movie_id, info_type.id AS info_type_id_3, info_type.info AS info_type_info_4, keyword.id AS keyword_id_5, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_info.info AS movie_info_info_9, movie_info.note AS note
 from movie_info,keyword,movie_companies,comp_cast_type,complete_cast,info_type,movie_keyword
 where (movie_info.movie_id = movie_companies.movie_id) And (movie_keyword.movie_id = movie_info.movie_id) And (info_type.id = movie_info.info_type_id) And (movie_info.note like '%internet%')  And  (movie_info.info like 'USA:% 200%') And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('nerd', 'loner', 'alienation', 'dignity')) And (movie_companies.movie_id = complete_cast.movie_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = 'complete+verified') And (info_type.info = 'release dates')