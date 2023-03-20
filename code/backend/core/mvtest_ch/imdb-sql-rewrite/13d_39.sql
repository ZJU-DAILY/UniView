SELECT MIN(mv39.name) AS producing_company,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS movie
FROM info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t,
     mv39
WHERE it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mv39.movie_id = t.id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mv39.movie_id
  AND miidx.movie_id = mv39.movie_id;

