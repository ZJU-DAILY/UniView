SELECT MIN(mi.info) AS movie_budget,
       MIN(mv132.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(t.title) AS violent_liongate_movie
FROM cast_info AS ci,
     company_name AS cn,
     info_type AS it1,
     keyword AS k,
     movie_companies AS mc,
     movie_info AS mi,
     movie_keyword AS mk,
     name AS n,
     title AS t,
     mv132
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mc.note LIKE '%(Blu-ray)%'
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.production_year > 2000
  AND (t.title LIKE '%Freddy%'
       OR t.title LIKE '%Jason%'
       OR t.title LIKE 'Saw%')
  AND t.id = mi.movie_id
  AND t.id = mv132.movie_id
  AND t.id = ci.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mv132.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mv132.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mv132.movie_id = mk.movie_id
  AND mv132.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id;

