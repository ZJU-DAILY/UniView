SELECT
  MIN(n.name) AS member_in_charnamed_american_movie,
  MIN(n.name) AS a1
FROM cast_info AS ci, name AS n, mv15
WHERE
  '[us]' = mv15.country_code
  AND n.name LIKE 'B%'
  AND ci.person_id = n.id
  AND ci.movie_id = mv15.movie_id