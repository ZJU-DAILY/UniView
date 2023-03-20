SELECT MIN(mv125.name) AS cool_actor_pseudonym,
       MIN(mv125.title) AS series_named_after_char
FROM mv125
WHERE mv125.country_code ='[us]'
  AND mv125.episode_nr < 100

