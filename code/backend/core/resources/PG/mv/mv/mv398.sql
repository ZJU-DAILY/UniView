create materialized view if not exists mv398 as select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS kind, complete_cast.movie_id AS movie_id, complete_cast.subject_id AS subject_id, keyword.id AS keyword_id_4, keyword.keyword AS keyword, title.kind_id AS kind_id, title.production_year AS production_year, title.title AS title
 from movie_keyword,title,comp_cast_type,complete_cast,keyword
 where (title.id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = 'complete') And (complete_cast.movie_id = movie_keyword.movie_id) And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence'))