select comp_cast_type.id AS id, comp_cast_type.kind AS kind, complete_cast.movie_id AS movie_id, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id
 from movie_companies,comp_cast_type,complete_cast
 where (movie_companies.movie_id = complete_cast.movie_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = 'complete+verified')