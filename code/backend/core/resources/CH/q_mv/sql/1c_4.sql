SELECT MIN(mv4.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM info_type AS it,
     movie_info_idx AS mi_idx,
     title AS t,
     mv4
WHERE it.info = 'top 250 rank'
  AND (mv4.note LIKE '%(co-production)%')
  AND t.production_year >2010
  AND t.id = mv4.movie_id
  AND t.id = mi_idx.movie_id
  AND mv4.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;

