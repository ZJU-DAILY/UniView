SELECT
  MIN(t.title) AS movie_title
FROM company_name AS cn, movie_companies AS mc, title AS t, mv12
WHERE
  '[nl]' = cn.country_code
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND mv12.movie_id = t.id
  AND mc.movie_id = mv12.movie_id