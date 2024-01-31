SELECT
  MIN(mv101.name_name_7) AS voicing_actress,
  MIN(t.title) AS jap_engl_voiced_movie
FROM aka_name AS an, company_name AS cn, info_type AS it, movie_companies AS mc, movie_info AS mi, title AS t, mv101
WHERE
  '[us]' = cn.country_code
  AND 'release dates' = it.info
  AND 'f' = mv101.gender
  AND t.production_year > 2000
  AND mi.movie_id = t.id
  AND mc.movie_id = t.id
  AND mv101.movie_id = t.id
  AND mc.movie_id = mv101.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = mv101.movie_id
  AND cn.id = mc.company_id
  AND it.id = mi.info_type_id
  AND an.person_id = mv101.person_id