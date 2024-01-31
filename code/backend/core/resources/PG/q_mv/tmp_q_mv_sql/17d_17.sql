SELECT
  MIN(n.name) AS member_in_charnamed_movie
FROM cast_info AS ci, company_name AS cn, name AS n, mv17
WHERE
  n.name LIKE '%Bert%'
  AND ci.person_id = n.id
  AND ci.movie_id = mv17.movie_id
  AND cn.id = mv17.company_id