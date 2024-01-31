select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, company_type.id AS company_type_id_2, complete_cast.movie_id AS movie_id, complete_cast.subject_id AS subject_id, keyword.id AS keyword_id_5, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.note AS note
 from movie_keyword,movie_companies,company_type,comp_cast_type,complete_cast,keyword
 where (complete_cast.movie_id = movie_keyword.movie_id) And (movie_companies.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (movie_companies.note not like '%(USA)%')  And  (movie_companies.note like '%(200%)%') And (company_type.id = movie_companies.company_type_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = '__NOTEQUAL__complete+verified') And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence'))