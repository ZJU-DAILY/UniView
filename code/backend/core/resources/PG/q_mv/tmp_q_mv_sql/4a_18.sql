SELECT
  MIN(mi_idx.info) AS rating,
  MIN(t.title) AS movie_title
FROM info_type AS it, movie_info_idx AS mi_idx, title AS t, mv18
WHERE
  'rating' = it.info
  AND mi_idx.info > '5.0'
  AND t.production_year > 2005
  AND mi_idx.movie_id = t.id
  AND mv18.movie_id = t.id
  AND mi_idx.movie_id = mv18.movie_id
  AND it.id = mi_idx.info_type_id