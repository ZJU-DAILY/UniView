SELECT MIN(cn.name) AS producing_company,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS complete_western_sequel
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     company_name AS cn,
     company_type AS ct,
     link_type AS lt,
     movie_companies AS mc,
     movie_info AS mi,
     movie_link AS ml,
     title AS t,
     mv19
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE 'complete%'
  AND cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'English')
  AND t.production_year BETWEEN 1950 AND 2010
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mv19.movie_id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND mi.movie_id = t.id
  AND t.id = cc.movie_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id
  AND ml.movie_id = mv19.movie_id
  AND ml.movie_id = mc.movie_id
  AND mv19.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mv19.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id
  AND ml.movie_id = cc.movie_id
  AND mv19.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = cc.movie_id;

