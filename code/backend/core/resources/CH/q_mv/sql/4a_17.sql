SELECT MIN(mv17.movie_info_idx_info_0) AS rating,
       MIN(t.title) AS movie_title
FROM keyword AS k,
     movie_keyword AS mk,
     title AS t,
     mv17
WHERE k.keyword LIKE '%sequel%'
  AND t.production_year > 2005
  AND t.id = mv17.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mv17.movie_id
  AND k.id = mk.keyword_id;

