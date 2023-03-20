SELECT MIN(n.name) AS member_in_charnamed_american_movie,
       MIN(n.name) AS a1
FROM cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     title AS t,
     mv119
WHERE cn.country_code ='[us]'
  AND n.name LIKE 'B%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mv119.movie_id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mv119.movie_id
  AND mc.movie_id = mv119.movie_id;

