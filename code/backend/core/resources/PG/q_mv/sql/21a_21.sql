SELECT MIN(cn.name) AS company_name,
       MIN(lt.link) AS link_type,
       MIN(t.title) AS western_follow_up
FROM company_name AS cn,
     keyword AS k,
     link_type AS lt,
     movie_info AS mi,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t,
     mv21
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND t.production_year BETWEEN 1950 AND 2000
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mv21.movie_id
  AND mv21.company_id = cn.id
  AND mi.movie_id = t.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mv21.movie_id
  AND mk.movie_id = mv21.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mv21.movie_id = mi.movie_id;

