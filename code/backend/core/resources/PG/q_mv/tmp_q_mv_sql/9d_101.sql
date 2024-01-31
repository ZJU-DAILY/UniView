SELECT
  MIN(an.name) AS alternative_name,
  MIN(mv101.char_name_name_5) AS voiced_char_name,
  MIN(mv101.name_name_7) AS voicing_actress,
  MIN(t.title) AS american_movie
FROM aka_name AS an, company_name AS cn, movie_companies AS mc, title AS t, mv101
WHERE
  '[us]' = cn.country_code
  AND 'f' = mv101.gender
  AND mv101.movie_id = t.id
  AND mc.movie_id = t.id
  AND mc.movie_id = mv101.movie_id
  AND cn.id = mc.company_id
  AND an.person_id = mv101.person_id