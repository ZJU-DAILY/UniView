SELECT MIN(cn.name) AS producing_company,
       MIN(mv64.info) AS rating,
       MIN(t.title) AS movie_about_winning
FROM company_name AS cn,
     company_type AS ct,
     info_type AS it2,
     kind_type AS kt,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t,
     mv64
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND t.title != ''
  AND (t.title LIKE '%Champion%'
       OR t.title LIKE '%Loser%')
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND mv64.movie_id = t.id
  AND mi.movie_id = mv64.movie_id
  AND mi.movie_id = mc.movie_id
  AND mv64.movie_id = mc.movie_id;

