SELECT MIN(mv120.title) AS movie_title
FROM company_name AS cn,
     movie_companies AS mc,
     mv120
WHERE cn.country_code ='[us]'
  AND cn.id = mc.company_id
  AND mc.movie_id = mv120.id
  AND mc.movie_id = mv120.movie_id;

