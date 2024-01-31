SELECT
  MIN(a1.name) AS writer_pseudo_name,
  MIN(t.title) AS movie_title
FROM aka_name AS a1, cast_info AS ci, name AS n1, role_type AS rt, title AS t, mv77
WHERE
  'writer' = rt.role
  AND a1.person_id = n1.id
  AND ci.person_id = n1.id
  AND ci.movie_id = t.id
  AND mv77.movie_id = t.id
  AND ci.role_id = rt.id
  AND a1.person_id = ci.person_id
  AND ci.movie_id = mv77.movie_id