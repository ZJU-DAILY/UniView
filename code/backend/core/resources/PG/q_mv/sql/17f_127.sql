SELECT MIN(n.name) AS member_in_charnamed_movie
FROM cast_info AS ci,
     name AS n,
     mv127
WHERE n.name LIKE '%B%'
  AND n.id = ci.person_id
  AND ci.movie_id = mv127.id
  AND ci.movie_id = mv127.movie_id
  AND ci.movie_id = mv127.movie_id;

