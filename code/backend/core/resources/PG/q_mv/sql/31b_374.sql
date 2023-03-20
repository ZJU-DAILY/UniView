SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(mv374.title) AS violent_liongate_movie
FROM cast_info AS ci,
     company_name AS cn,
     info_type AS it1,
     info_type AS it2,
     movie_companies AS mc,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     name AS n,
     mv374
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND mc.note LIKE '%(Blu-ray)%'
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND mv374.production_year > 2000
  AND (mv374.title LIKE '%Freddy%'
       OR mv374.title LIKE '%Jason%'
       OR mv374.title LIKE 'Saw%')
  AND mv374.id = mi.movie_id
  AND mv374.id = mi_idx.movie_id
  AND mv374.id = ci.movie_i
  AND mv374.id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mv374.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mv374.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mv374.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mv374.movie_id = mc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND cn.id = mc.company_id;

