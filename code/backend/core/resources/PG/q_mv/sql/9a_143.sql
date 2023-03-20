SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS character_name,
       MIN(t.title) AS movie
FROM aka_name AS an,
     char_name AS chn,
     company_name AS cn,
     movie_companies AS mc,
     title AS t,
     mv143
WHERE cn.country_code ='[us]'
  AND mc.note IS NOT NULL
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND mv143.gender ='f'
  AND mv143.name LIKE '%Ang%'
  AND t.production_year BETWEEN 2005 AND 2015
  AND mv143.movie_id = t.id
  AND t.id = mc.movie_id
  AND mv143.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND chn.id = mv143.person_role_id
  AND an.person_id = mv143.id
  AND an.person_id = mv143.person_id;

