SELECT MIN(an.name) AS alternative_name,
       MIN(chn.name) AS voiced_char_name,
       MIN(mv143.name) AS voicing_actress,
       MIN(t.title) AS american_movie
FROM aka_name AS an,
     char_name AS chn,
     company_name AS cn,
     movie_companies AS mc,
     title AS t,
     mv143
WHERE cn.country_code ='[us]'
  AND mv143.gender ='f'
  AND mv143.movie_id = t.id
  AND t.id = mc.movie_id
  AND mv143.movie_id = mc.movie_id
  AND mc.company_id = cn.id
  AND chn.id = mv143.person_role_id
  AND an.person_id = mv143.id
  AND an.person_id = mv143.person_id;

