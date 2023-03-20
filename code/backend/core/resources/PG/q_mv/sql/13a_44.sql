SELECT MIN(mi.info) AS release_date,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS german_movie
FROM company_name AS cn,
     info_type AS it,
     info_type AS it2,
     kind_type AS kt,
     movie_companies AS mv44,
     movie_info AS mi,
     movie_info_idx AS miidx,
     title AS t,
     mv44
WHERE cn.country_code ='[de]'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mv44.movie_id = t.id
  AND cn.id = mv44.company_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mv44.movie_id
  AND miidx.movie_id = mv44.movie_id;

