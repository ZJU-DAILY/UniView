SELECT MIN(mv95.info) AS release_date,
       MIN(t.title) AS youtube_movie
FROM aka_title AS at,
     company_name AS cn,
     company_type AS ct,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t,
     mv95
WHERE cn.country_code = '[us]'
  AND cn.name = 'YouTube'
  AND mc.note LIKE '%(200%)%'
  AND mc.note LIKE '%(worldwide)%'
  AND t.production_year BETWEEN 2005 AND 2010
  AND t.id = at.movie_id
  AND t.id = mv95.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mv95.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mv95.movie_id = mc.movie_id
  AND mv95.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

