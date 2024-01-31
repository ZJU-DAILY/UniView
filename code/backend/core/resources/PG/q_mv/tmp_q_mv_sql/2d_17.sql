SELECT
  MIN(mv17.title) AS movie_title
FROM company_name AS cn, mv17
WHERE
  '[us]' = cn.country_code AND cn.id = mv17.company_id