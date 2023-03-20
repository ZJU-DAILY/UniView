SELECT MIN(t.title) AS movie_title
FROM keyword AS k,
     movie_keyword AS mk,
     title AS t,
     mv10
WHERE k.keyword ='character-name-in-title'
  AND mv10.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mv10.movie_id = mk.movie_id;

