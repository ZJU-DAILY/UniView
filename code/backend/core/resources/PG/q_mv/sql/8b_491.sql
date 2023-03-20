SELECT MIN(an.name) AS acress_pseudonym,
       MIN(t.title) AS japanese_anime_movie
FROM aka_name AS an,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     role_type AS rt,
     title AS t,
     mv491
WHERE cn.country_code ='[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND (mc.note LIKE '%(2006)%'
       OR mc.note LIKE '%(2007)%')
  AND n.name LIKE '%Yo%'
  AND n.name NOT LIKE '%Yu%'
  AND t.production_year BETWEEN 2006 AND 2007
  AND (t.title LIKE 'One Piece%'
       OR t.title LIKE 'Dragon Ball Z%')
  AND an.person_id = n.id
  AND n.id = mv491.person_id
  AND mv491.movie_id = t.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an.person_id = mv491.person_id
  AND mv491.movie_id = mc.movie_id;

