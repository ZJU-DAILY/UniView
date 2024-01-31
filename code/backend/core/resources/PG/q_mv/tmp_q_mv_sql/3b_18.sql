SELECT
  MIN(t.title) AS movie_title
FROM movie_info AS mi, title AS t, mv18
WHERE
  mi.info IN ('Bulgaria')
  AND t.production_year > 2010
  AND mi.movie_id = t.id
  AND mv18.movie_id = t.id
  AND mi.movie_id = mv18.movie_id