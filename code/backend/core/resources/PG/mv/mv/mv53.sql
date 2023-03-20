create materialized view if not exists mv53 as select link_type.id AS id, link_type.link AS link, movie_link.linked_movie_id AS linked_movie_id, title.production_year AS production_year, title.title AS title
 from link_type,title,movie_link
 where (movie_link.link_type_id = link_type.id) And (link_type.link = 'features') And (title.id = movie_link.linked_movie_id)