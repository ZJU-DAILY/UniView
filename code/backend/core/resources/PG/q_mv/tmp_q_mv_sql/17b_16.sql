SELECT
  MIN(n.name) AS member_in_charnamed_movie,
  MIN(n.name) AS a1
FROM cast_info AS ci, company_name AS cn, movie_companies AS mc, name AS n, mv16
WHERE
  n.name LIKE 'Z%'
  AND ci.person_id = n.id
  AND ci.movie_id = mv16.movie_id
  AND mc.movie_id = mv16.movie_id
  AND cn.id = mc.company_id
  AND ci.movie_id = mc.movie_id