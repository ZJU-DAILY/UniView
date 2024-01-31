SELECT
  MIN(n.name) AS voicing_actress,
  MIN(t.title) AS jap_engl_voiced_movie
FROM aka_name AS an, char_name AS chn, company_name AS cn, info_type AS it, movie_companies AS mc, movie_info AS mi, name AS n, title AS t, mv87
WHERE
  '[us]' = cn.country_code
  AND 'release dates' = it.info
  AND 'f' = n.gender
  AND t.production_year > 2000
  AND mi.movie_id = t.id
  AND mc.movie_id = t.id
  AND mv87.movie_id = t.id
  AND mc.movie_id = mv87.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = mv87.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND mv87.person_id = n.id
  AND an.person_id = n.id
  AND an.person_id = mv87.person_id
  AND chn.id = mv87.person_role_id