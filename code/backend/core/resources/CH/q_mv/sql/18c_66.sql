SELECT MIN(mv66.movie_info_info_2) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(t.title) AS movie_title
FROM info_type AS it2,
     movie_info_idx AS mi_idx,
     name AS n,
     title AS t,
     mv66
WHERE mv66.movie_info_info_2 = 'genres'
  AND it2.info = 'votes'
  AND n.gender = 'm'
  AND t.id = mv66.movie_id
  AND t.id = mi_idx.movie_id
  AND t.id = mv66.movie_id
  AND mv66.movie_id = mi_idx.movie_id
  AND mv66.movie_id = mi_idx.movie_id
  AND n.id = mv66.person_id
  AND it2.id = mi_idx.info_type_id;

