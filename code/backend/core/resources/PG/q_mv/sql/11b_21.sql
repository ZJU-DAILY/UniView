SELECT MIN(cn.name) AS from_company,
       MIN(lt.link) AS movie_link_type,
       MIN(t.title) AS sequel_movie
FROM company_name AS cn,
     keyword AS k,
     link_type AS lt,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t,
     mv21
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follows%'
  AND t.production_year = 1998
  AND t.title LIKE '%Money%'
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mv21.movie_id
  AND mv21.company_id = cn.id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mv21.movie_id
  AND mk.movie_id = mv21.movie_id;

