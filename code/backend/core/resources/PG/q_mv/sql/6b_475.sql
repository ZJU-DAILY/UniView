SELECT MIN(mv475.keyword) AS movie_keyword,
       MIN(mv475.name) AS actor_name,
       MIN(mv475.title) AS hero_movie
FROM mv475
WHERE mv475.name LIKE '%Downey%Robert%'
  AND mv475.production_year > 2014

