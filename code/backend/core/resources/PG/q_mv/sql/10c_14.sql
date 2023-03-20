SELECT MIN(chn.name) AS character,
       MIN(t.title) AS movie_with_american_producer
FROM char_name AS chn,
     cast_info AS ci,
     company_type AS ct,
     role_type AS rt,
     title AS t,
     mv14
WHERE ci.note LIKE '%(producer)%'
  AND t.production_year > 1990
  AND t.id = mv14.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mv14.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND ct.id = mv14.company_type_id;
