SELECT MIN(n.name) AS member_in_charnamed_movie,
       MIN(n.name) AS a1
FROM cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     mv120
WHERE n.name LIKE 'Z%'
  AND n.id = ci.person_id
  AND ci.movie_id = mv120.id
  AND mv120.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mv120.movie_id
  AND mc.movie_id = mv120.movie_id;

