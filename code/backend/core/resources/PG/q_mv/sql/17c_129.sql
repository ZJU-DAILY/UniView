SELECT MIN(mv129.name) AS member_in_charnamed_movie,
       MIN(mv129.name) AS a1
FROM mv129
WHERE mv129.name LIKE 'X%';

