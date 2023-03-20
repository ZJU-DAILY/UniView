SELECT MIN(mv7.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM info_type AS it,
     movie_info_idx AS mi_idx,
     title AS t,
     mv7
WHERE it.info = 'top 250 rank'
  AND t.production_year >2010
  AND t.id = mv7.movie_id
  AND t.id = mi_idx.movie_id
  AND mv7.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;

