SELECT
  MIN(mv47.keyword) AS movie_keyword,
  MIN(n.name) AS actor_name,
  MIN(t.title) AS hero_movie
FROM cast_info AS ci, name AS n, title AS t, mv47
WHERE
  t.production_year > 2000
  AND mv47.movie_id = t.id
  AND ci.movie_id = t.id
  AND ci.movie_id = mv47.movie_id
  AND ci.person_id = n.id