SELECT MIN(cn.name) AS company_name,
       MIN(mv18.link) AS link_type,
       MIN(t.title) AS german_follow_up
FROM company_name AS cn,
     company_type AS ct,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t,
     mv18
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND mc.note IS NULL
  AND mi.info IN ('Germany',
                  'German')
  AND t.production_year BETWEEN 2000 AND 2010
  AND mv18.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND mv18.movie_id = mk.movie_id
  AND mv18.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND mv18.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id;

