SELECT
  MIN(mv16.title) AS movie_title
FROM company_name AS cn, movie_companies AS mc, mv16
WHERE
  '[us]' = cn.country_code AND cn.id = mc.company_id AND mc.movie_id = mv16.movie_id