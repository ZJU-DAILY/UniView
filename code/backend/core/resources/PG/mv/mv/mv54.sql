create materialized view if not exists mv54 as select cast_info.movie_id AS movie_id, cast_info.person_id AS person_id, link_type.id AS id, link_type.link AS link, title.production_year AS production_year, title.title AS title
 from link_type,cast_info,title,movie_link
 where (movie_link.link_type_id = link_type.id) And (link_type.link = 'features') And (cast_info.movie_id = title.id) And (title.id = movie_link.linked_movie_id)