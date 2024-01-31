SELECT
  MIN(an1.name) AS costume_designer_pseudo,
  MIN(t.title) AS movie_with_costumes
FROM aka_name AS an1, cast_info AS ci, name AS n1, role_type AS rt, title AS t, mv77
WHERE
  'costume designer' = rt.role
  AND an1.person_id = n1.id
  AND ci.person_id = n1.id
  AND ci.movie_id = t.id
  AND mv77.movie_id = t.id
  AND ci.role_id = rt.id
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mv77.movie_id