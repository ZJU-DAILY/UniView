SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM info_type AS it,
     movie_info_idx AS mi_idx,
     title AS t,
     mv443
WHERE it.info ='rating'
  AND mi_idx.info > '2.0'
  AND t.production_year > 1990
  AND t.id = mi_idx.movie_id
  AND t.id = mv443.movie_id
  AND mv443.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;

