select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, complete_cast.movie_id AS complete_cast_movie_id_2, complete_cast.status_id AS status_id, info_type.id AS info_type_id_4, info_type.info AS info_type_info_5, keyword.id AS keyword_id_6, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_companies.note AS note, movie_info.info AS movie_info_info_11, movie_info.movie_id AS movie_info_movie_id_12, title.kind_id AS kind_id, title.production_year AS production_year, title.title AS title
 from info_type,title,comp_cast_type,movie_keyword,keyword,movie_companies,complete_cast,movie_info
 where (movie_info.info_type_id = info_type.id) And (info_type.info = 'countries') And (title.id = movie_keyword.movie_id) And (movie_info.movie_id = title.id) And (comp_cast_type.id = complete_cast.subject_id) And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_companies.movie_id = movie_keyword.movie_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence')) And (movie_companies.note not like '%(USA)%')  And  (movie_companies.note like '%(200%)%') And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American'))