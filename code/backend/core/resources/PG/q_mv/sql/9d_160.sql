SELECT MIN(an.name) AS alternative_name,
       MIN(mv160.name) AS voiced_char_name,
       MIN(mv160.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM aka_name AS an,
     company_name AS cn,
     movie_companies AS mc,
     title AS t,
     mv160
WHERE cn.country_code ='[us]'
  AND mv160.gender ='f'
  AND mv160.movie_id = t.id
  AND t.id = mc.movie_id
  AND mv160.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND mv160.role_id = mv160.id
  AND an.person_id = mv160.id
  AND an.person_id = mv160.person_id;

