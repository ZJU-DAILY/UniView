SELECT MIN(cn.name) AS from_company,
       MIN(lt.link) AS movie_link_type,
       MIN(t.title) AS non_polish_sequel_movie
FROM company_name AS cn,
     company_type AS ct,
     link_type AS lt,
     movie_companies AS mc,
     movie_link AS ml,
     title AS t,
     mv19
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
       OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND t.production_year BETWEEN 1950 AND 2000
  AND lt.id = ml.link_type_id
  AND ml.movie_id = t.id
  AND t.id = mv19.movie_id
  AND t.id = mc.movie_id
  AND mc.company_type_id = ct.id
  AND mc.company_id = cn.id
  AND ml.movie_id = mv19.movie_id
  AND ml.movie_id = mc.movie_id
  AND mv19.movie_id = mc.movie_id;

