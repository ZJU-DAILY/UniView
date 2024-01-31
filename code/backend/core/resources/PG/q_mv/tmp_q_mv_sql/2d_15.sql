SELECT
  MIN(mv15.title) AS movie_title
FROM mv15
WHERE
  '[us]' = mv15.country_code