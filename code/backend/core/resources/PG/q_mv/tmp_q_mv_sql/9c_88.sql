SELECT
  MIN(an.name) AS alternative_name,
  MIN(chn.name) AS voiced_character_name,
  MIN(mv88.name) AS voicing_actress,
  MIN(t.title) AS american_movie
FROM aka_name AS an, char_name AS chn, company_name AS cn, movie_companies AS mc, title AS t, mv88
WHERE
  '[us]' = cn.country_code
  AND 'f' = mv88.gender
  AND mv88.name LIKE '%An%'
  AND mv88.movie_id = t.id
  AND mc.movie_id = t.id
  AND mc.movie_id = mv88.movie_id
  AND cn.id = mc.company_id
  AND chn.id = mv88.person_role_id
  AND an.person_id = mv88.person_id