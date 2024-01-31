SELECT
  MIN(an.name) AS cool_actor_pseudonym,
  MIN(mv16.title) AS series_named_after_char
FROM aka_name AS an, cast_info AS ci, company_name AS cn, movie_companies AS mc, name AS n, mv16
WHERE
  '[us]' = cn.country_code
  AND mv16.episode_nr < 100
  AND an.person_id = n.id
  AND ci.person_id = n.id
  AND ci.movie_id = mv16.movie_id
  AND mc.movie_id = mv16.movie_id
  AND cn.id = mc.company_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id