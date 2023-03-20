SELECT MIN(lt.link) AS link_type,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM link_type AS lt,
     movie_link AS ml,
     title AS t1,
     title AS t2,
     mv70
WHERE t1.id = mv70.movie_id
  AND ml.movie_id = t1.id
  AND ml.linked_movie_id = t2.id
  AND lt.id = ml.link_type_id
  AND mv70.movie_id = t1.id;

