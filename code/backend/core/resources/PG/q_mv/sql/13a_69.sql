SELECT MIN(mv69.info) AS release_date,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS german_movie
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info_idx AS miidx,
     title AS t,
     mv69
WHERE cn.country_code ='[de]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND kt.kind ='movie'
  AND mv69.movie_id = t.id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mv69.movie_id = miidx.movie_id
  AND mv69.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

