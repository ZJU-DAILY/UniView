SELECT MIN(an.name) AS cool_actor_pseudonym,
       MIN(mv120.title) AS series_named_after_char
FROM aka_name AS an,
     cast_info AS ci,
     company_name AS cn,
     movie_companies AS mc,
     name AS n,
     mv120
WHERE cn.country_code ='[us]'
  AND mv120.episode_nr < 100
  AND an.person_id = n.id
  AND n.id = ci.person_id
  AND ci.movie_id = mv120.id
  AND mv120.id = mc.movie_id
  AND mc.company_id = cn.id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mv120.movie_id
  AND mc.movie_id = mv120.movie_id;

