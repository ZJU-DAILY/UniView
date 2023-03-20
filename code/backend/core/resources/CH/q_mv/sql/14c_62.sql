SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS north_european_dark_production
FROM info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t,
     mv62
WHERE mv62.movie_info_info_0 = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IS NOT NULL
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.id = t.kind_id
  AND t.id = mv62.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = mv62.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mv62.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it2.id = mi_idx.info_type_id;

