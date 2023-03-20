SELECT MIN(t.title) AS movie_title
FROM company_name AS cn,
     movie_companies AS mc,
     title AS t,
     mv119
WHERE cn.country_code ='[us]'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mv119.movie_id
  AND mc.movie_id = mv119.movie_id;

