SELECT
  MIN(chn.name) AS character,
  MIN(t.title) AS movie_with_american_producer
FROM char_name AS chn, cast_info AS ci, company_type AS ct, role_type AS rt, title AS t, mv77
WHERE
  ci.note LIKE '%(producer)%'
  AND t.production_year > 1990
  AND mv77.movie_id = t.id
  AND ci.movie_id = t.id
  AND ci.movie_id = mv77.movie_id
  AND chn.id = ci.person_role_id
  AND ci.role_id = rt.id
  AND ct.id = mv77.company_type_id