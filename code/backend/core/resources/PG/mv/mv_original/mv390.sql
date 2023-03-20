select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, company_type.id AS company_type_id_2, complete_cast.movie_id AS movie_id, keyword.id AS keyword_id_4, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.note AS note, title.kind_id AS kind_id, title.production_year AS production_year, title.title AS title
 from company_type,title,comp_cast_type,movie_keyword,keyword,movie_companies,complete_cast
 where (company_type.id = movie_companies.company_type_id) And (title.id = movie_keyword.movie_id) And (comp_cast_type.id = complete_cast.subject_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = '__NOTEQUAL__complete+verified') And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_companies.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence')) And (movie_companies.note not like '%(USA)%')  And  (movie_companies.note like '%(200%)%')