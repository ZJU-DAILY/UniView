select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, complete_cast.movie_id AS movie_id, complete_cast.status_id AS status_id, info_type.id AS info_type_id_4, info_type.info AS info_type_info_5, keyword.id AS keyword_id_6, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_companies.note AS note, movie_info.info AS movie_info_info_11
 from movie_keyword,movie_info,movie_companies,comp_cast_type,complete_cast,info_type,keyword
 where (complete_cast.movie_id = movie_keyword.movie_id) And (movie_companies.movie_id = movie_keyword.movie_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (movie_info.info_type_id = info_type.id) And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American')) And (movie_companies.note not like '%(USA)%')  And  (movie_companies.note like '%(200%)%') And (comp_cast_type.id = complete_cast.subject_id) And (info_type.info = 'countries') And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence'))