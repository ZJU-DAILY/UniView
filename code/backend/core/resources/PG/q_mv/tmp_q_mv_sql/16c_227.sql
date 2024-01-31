SELECT
  MIN(mv227.name) AS cool_actor_pseudonym,
  MIN(mv227.title) AS series_named_after_char
FROM company_name AS cn, movie_companies AS mc, mv227
WHERE
  '[us]' = cn.country_code
  AND mv227.episode_nr < 100
  AND mc.movie_id = mv227.cast_info_movie_id_2
  AND cn.id = mc.company_id