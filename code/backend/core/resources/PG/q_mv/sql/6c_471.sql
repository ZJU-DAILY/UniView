SELECT MIN(mv471.keyword) AS movie_keyword,
       MIN(mv471.name) AS actor_name,
       MIN(mv471.title) AS marvel_movie
FROM mv471
WHERE mv471.name LIKE '%Downey%Robert%'
  AND mv471.production_year > 2014

