SELECT MIN(n.name) AS member_in_charnamed_american_movie,
       MIN(n.name) AS a1
FROM cast_info AS ci,
     name AS n,
     mv127
WHERE mv127.country_code ='[us]'
  AND n.name LIKE 'B%'
  AND n.id = ci.person_id
  AND ci.movie_id = mv127.id
  AND ci.movie_id = mv127.movie_id
  AND ci.movie_id = mv127.movie_id;

