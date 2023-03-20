SELECT MIN(an1.name) AS actress_pseudonym,
       MIN(t.title) AS japanese_movie_dubbed
FROM aka_name AS an1,
     company_name AS cn,
     movie_companies AS mc,
     name AS n1,
     title AS t,
     mv491
WHERE cn.country_code ='[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND n1.name LIKE '%Yo%'
  AND n1.name NOT LIKE '%Yu%'
  AND an1.person_id = n1.id
  AND n1.id = mv491.person_id
  AND mv491.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an1.person_id = mv491.person_id
  AND mv491.movie_id = mc.movie_id;

