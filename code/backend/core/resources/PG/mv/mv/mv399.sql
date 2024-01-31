create materialized view if not exists mv399 as select comp_cast_type.id AS comp_cast_type_id_0, comp_cast_type.kind AS comp_cast_type_kind_1, complete_cast.movie_id AS movie_id, complete_cast.subject_id AS subject_id, keyword.id AS keyword_id_4, keyword.keyword AS keyword, kind_type.id AS kind_type_id_6, kind_type.kind AS kind_type_kind_7, title.production_year AS production_year, title.title AS title
 from movie_keyword,title,comp_cast_type,complete_cast,kind_type,keyword
 where (title.id = movie_keyword.movie_id) And (complete_cast.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (kind_type.id = title.kind_id) And (complete_cast.status_id = comp_cast_type.id) And (comp_cast_type.kind = 'complete') And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence'))