select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, complete_cast.movie_id AS movie_id, complete_cast.status_id AS status_id, keyword.id AS keyword_id_4, keyword.keyword AS keyword
 from keyword,complete_cast,comp_cast_type,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword in ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw', 'laser')) And (complete_cast.subject_id = comp_cast_type.id) And (complete_cast.movie_id = movie_keyword.movie_id) And (comp_cast_type.kind = 'cast')