SELECT
  MIN(mv43.keyword) AS movie_keyword,
  MIN(n.name) AS actor_name,
  MIN(t.title) AS marvel_movie
FROM cast_info AS ci, name AS n, title AS t, mv43
WHERE
  n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2010
  AND mv43.movie_id = t.id
  AND ci.movie_id = t.id
  AND ci.movie_id = mv43.movie_id
  AND ci.person_id = n.id