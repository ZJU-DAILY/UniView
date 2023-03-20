create materialized view if not exists mv133 as select link_type.id AS id, link_type.link AS link, movie_link.movie_id AS movie_id
 from link_type,movie_link
 where (movie_link.link_type_id = link_type.id) And (link_type.link like '%follows%')