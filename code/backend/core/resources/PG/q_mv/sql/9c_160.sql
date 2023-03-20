SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_character_name,
       MIN(n.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM aka_name AS an,
     char_name AS chn,
     movie_companies AS mc,
     title AS t,
     mv160
WHERE mv160.country_code ='[us]'
  AND mv160.gender ='f'
  AND mv160.name LIKE '%An%'
  AND mv160.movie_id = t.id
  AND t.id = mc.movie_id
  AND mv160.movie_id = mc.movie_id
  AND mc.company_id = mv160.id
  AND chn.id = mv160.person_role_id
  AND an.person_id = mv160.id
  AND an.person_id = mv160.person_id;

