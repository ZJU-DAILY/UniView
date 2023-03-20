SELECT MIN(t.title) AS movie_title
FROM title AS t,
     mv13
WHERE t.production_year > 2010
  AND t.id = mv13.movie_id
  AND t.id = mv13.movie_id;

