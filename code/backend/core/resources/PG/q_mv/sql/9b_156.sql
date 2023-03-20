SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_character,
       MIN(mv156.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM char_name AS chn,
     company_name AS cn,
     movie_companies AS mc,
     title AS t,
     mv156
WHERE cn.country_code ='[us]'
  AND mc.note LIKE '%(200%)%'
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND mv156.gender ='f'
  AND mv156.name LIKE '%Angel%'
  AND t.production_year BETWEEN 2007 AND 2010
  AND mv156.movie_id = t.id
  AND t.id = mc.movie_id
  AND mv156.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND chn.id = mv156.person_role_id

