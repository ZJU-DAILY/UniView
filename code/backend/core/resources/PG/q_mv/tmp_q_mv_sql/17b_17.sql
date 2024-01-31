SELECT
  MIN(n.name) AS member_in_charnamed_movie,
  MIN(n.name) AS a1
FROM cast_info AS ci, company_name AS cn, name AS n, mv17
WHERE
  n.name LIKE 'Z%'
  AND ci.person_id = n.id
  AND ci.movie_id = mv17.movie_id
  AND cn.id = mv17.company_id