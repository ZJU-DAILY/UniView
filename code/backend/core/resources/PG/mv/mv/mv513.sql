create materialized view if not exists mv513 as select link_type.id AS id, link_type.link AS link, movie_link.linked_movie_id AS linked_movie_id, movie_link.movie_id AS movie_id
 from link_type,movie_link
 where (movie_link.link_type_id = link_type.id) And (link_type.link in ('sequel', 'follows', 'followed by'))