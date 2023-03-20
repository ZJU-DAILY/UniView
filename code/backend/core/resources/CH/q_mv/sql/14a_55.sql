SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS northern_dark_movie
FROM info_type AS it2,
     keyword AS k,
     kind_type AS kt,
     movie_info_idx AS mi_idx,
     movie_keyword AS mk,
     title AS t,
     mv55
WHERE mv55.info_type_info_3 = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind = 'movie'
  AND mi_idx.info < '8.5'
  AND t.production_year > 2010
  AND kt.id = t.kind_id
  AND t.id = mv55.movie_id
  AND t.id = mk.movie_id
  AND t.id = mi_idx.movie_id
  AND mk.movie_id = mv55.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mv55.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it2.id = mi_idx.info_type_id;

