SELECT MIN(mv35.name) AS character,
       MIN(t.title) AS russian_mov_with_actor_producer
FROM company_name AS cn,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t,
     mv35
WHERE cn.country_code = '[ru]'
  AND rt.role = 'actor'
  AND t.production_year > 2010
  AND t.id = mc.movie_id
  AND t.id = mv35.movie_id
  AND mv35.movie_id = mc.movie_id
  AND rt.id = mv35.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;

