SELECT MIN(mv160.name) AS voicing_actress,
       MIN(t.title) AS jap_engl_voiced_movie
FROM aka_name AS an,
     company_name AS cn,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t,
     mv160
WHERE cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
       OR mi.info LIKE 'USA:%200%')
  AND mv160.gender ='f'
  AND mv160.name LIKE '%An%'
  AND t.production_year > 2000
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = mv160.movie_id
  AND mc.movie_id = mv160.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = mv160.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND mv160.id = an.person_id
  AND mv160.person_id = an.person_id;

