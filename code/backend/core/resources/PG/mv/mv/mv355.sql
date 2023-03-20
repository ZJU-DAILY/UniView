create materialized view if not exists mv355 as select complete_cast.movie_id AS movie_id, complete_cast.status_id AS status_id, complete_cast.subject_id AS subject_id, keyword.id AS id, keyword.keyword AS keyword
 from keyword,complete_cast,movie_keyword
 where (movie_keyword.keyword_id = keyword.id) And (keyword.keyword = 'sequel') And (complete_cast.movie_id = movie_keyword.movie_id)