SELECT
  MIN(n.name) AS voicing_actress,
  MIN(t.title) AS jap_engl_voiced_movie
FROM aka_name AS an, char_name AS chn, cast_info AS ci, company_name AS cn, movie_companies AS mc, name AS n, role_type AS rt, title AS t, mv175
WHERE
  ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')
  AND '[us]' = cn.country_code
  AND 'f' = n.gender
  AND 'actress' = rt.role
  AND t.production_year > 2000
  AND mv175.movie_id = t.id
  AND mc.movie_id = t.id
  AND ci.movie_id = t.id
  AND ci.movie_id = mc.movie_id
  AND mc.movie_id = mv175.movie_id
  AND ci.movie_id = mv175.movie_id
  AND cn.id = mc.company_id
  AND ci.person_id = n.id
  AND ci.role_id = rt.id
  AND an.person_id = n.id
  AND an.person_id = ci.person_id
  AND chn.id = ci.person_role_id