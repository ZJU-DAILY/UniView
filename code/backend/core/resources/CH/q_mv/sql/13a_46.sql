SELECT MIN(mi.info) AS release_date,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS german_movie
FROM info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t,
     mv46
WHERE it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mv46.movie_id = t.id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mv46.movie_id
  AND miidx.movie_id = mv46.movie_id;

