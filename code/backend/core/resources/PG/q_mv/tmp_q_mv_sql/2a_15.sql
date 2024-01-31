SELECT
  MIN(mv15.title) AS movie_title
FROM mv15
WHERE
  '[de]' = mv15.country_code