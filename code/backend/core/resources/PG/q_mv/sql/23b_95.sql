SELECT MIN(kt.kind) AS movie_kind,
       MIN(t.title) AS complete_nerdy_internet_movie
FROM complete_cast AS cc,
     comp_cast_type AS cct1,
     company_name AS cn,
     company_type AS ct,
     keyword AS k,
     kind_type AS kt,
     movie_companies AS mc,
     movie_keyword AS mk,
     title AS t,
     mv95
WHERE cct1.kind = 'complete+verified'
  AND cn.country_code = '[us]'
  AND k.keyword IN ('nerd',
                    'loner',
                    'alienation',
                    'dignity')
  AND kt.kind IN ('movie')
  AND t.production_year > 2000
  AND kt.id = t.kind_id
  AND t.id = mv95.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND t.id = cc.movie_id
  AND mk.movie_id = mv95.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mv95.movie_id = mc.movie_id
  AND mv95.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND k.id = mk.keyword_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND cct1.id = cc.status_id;

