create materialized view if not exists mv42 as select company_type.id AS id, company_type.kind AS kind, movie_companies.note AS note, movie_info.info AS info, movie_info.movie_id AS movie_id, title.production_year AS production_year, title.title AS title
 from company_type,info_type,title,movie_companies,movie_info
 where (company_type.id = movie_companies.company_type_id) And (company_type.kind = 'production companies') And (info_type.id = movie_info.info_type_id) And (title.id = movie_info.movie_id) And (movie_info.movie_id = movie_companies.movie_id) And (movie_companies.note not like '%(TV)%')  And  (movie_companies.note like '%(USA)%') And (movie_info.info_type_id = info_type.id) And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American'))