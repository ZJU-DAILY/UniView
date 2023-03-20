SELECT MIN(mv39.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS mainstream_movie
FROM info_type AS it1,
     info_type AS it2,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     title AS t,
     mv39
WHERE it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama',
                  'Horror',
                  'Western',
                  'Family')
  AND mi_idx.info > '7.0'
  AND t.production_year BETWEEN 2000 AND 2010
  AND t.id = mi.movie_id
  AND t.id = mi_idx.movie_id
  AND mi.info_type_id = it1.id
  AND mi_idx.info_type_id = it2.id
  AND t.id = mv39.movie_id
  AND mv39.movie_id = mi.movie_id
  AND mv39.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

