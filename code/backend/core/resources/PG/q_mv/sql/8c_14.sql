SELECT MIN(a1.name) AS writer_pseudo_name,
       MIN(t.title) AS movie_title
FROM aka_name AS a1,
     cast_info AS ci,
     name AS n1,
     role_type AS rt,
     title AS t,
     mv14
WHERE rt.role ='writer'
  AND a1.person_id = n1.id
  AND n1.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mv14.movie_id
  AND ci.role_id = rt.id
  AND a1.person_id = ci.person_id
  AND ci.movie_id = mv14.movie_id;

