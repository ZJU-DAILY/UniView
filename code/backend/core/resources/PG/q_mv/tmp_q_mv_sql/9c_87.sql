SELECT
  MIN(an.name) AS alternative_name,
  MIN(chn.name) AS voiced_character_name,
  MIN(n.name) AS voicing_actress,
  MIN(t.title) AS american_movie
FROM aka_name AS an, char_name AS chn, company_name AS cn, movie_companies AS mc, name AS n, title AS t, mv87
WHERE
  '[us]' = cn.country_code
  AND 'f' = n.gender
  AND n.name LIKE '%An%'
  AND mv87.movie_id = t.id
  AND mc.movie_id = t.id
  AND mc.movie_id = mv87.movie_id
  AND cn.id = mc.company_id
  AND mv87.person_id = n.id
  AND chn.id = mv87.person_role_id
  AND an.person_id = n.id
  AND an.person_id = mv87.person_id