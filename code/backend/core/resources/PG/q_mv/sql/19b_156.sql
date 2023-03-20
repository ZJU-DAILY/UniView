SELECT MIN(mv156.name) AS voicing_actress,
       MIN(t.title) AS kung_fu_panda
FROM char_name AS chn,
     company_name AS cn,
     info_type AS it,
     movie_companies AS mc,
     movie_info AS mi,
     title AS t,
     mv156
WHERE cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND mc.note LIKE '%(200%)%'
  AND (mc.note LIKE '%(USA)%'
       OR mc.note LIKE '%(worldwide)%')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%2007%'
       OR mi.info LIKE 'USA:%2008%')
  AND mv156.gender ='f'
  AND mv156.name LIKE '%Angel%'
  AND t.production_year BETWEEN 2007 AND 2008
  AND t.title LIKE '%Kung%Fu%Panda%'
  AND t.id = mi.movie_id
  AND t.id = mc.movie_id
  AND t.id = mv156.movie_id
  AND mc.movie_id = mv156.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = mv156.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND chn.id = mv156.person_role_id;

