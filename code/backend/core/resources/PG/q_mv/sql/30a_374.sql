SELECT MIN(mi.info) AS movie_budget,
       MIN(mi_idx.info) AS movie_votes,
       MIN(n.name) AS writer,
       MIN(mv374.title) AS complete_violent_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     comp_cast_type AS cct2,
     cast_info AS ci,
     info_type AS it1,
     info_type AS it2,
     movie_info AS mi,
     movie_info_idx AS mi_idx,
     name AS n,
     mv374
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND mv374.production_year > 2000
  AND mv374.id = mi.movie_id
  AND mv374.id = mi_idx.movie_id
  AND mv374.id = ci.movie_id
  AND mv374.id = cc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mv374.movie_id
  AND ci.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mv374.movie_id
  AND mi.movie_id = cc.movie_id
  AND mi_idx.movie_id = mv374.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND mv374.movie_id = cc.movie_id
  AND n.id = ci.person_id
  AND it1.id = mi.info_type_id
  AND it2.id = mi_idx.info_type_id
  AND cct1.id = cc.subject_id
  AND cct2.id = cc.status_id;

