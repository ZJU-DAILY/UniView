create materialized view if not exists mv376 as select complete_cast.movie_id AS movie_id, complete_cast.status_id AS status_id, complete_cast.subject_id AS subject_id, keyword.id AS id, keyword.keyword AS keyword, movie_companies.company_id AS company_id, movie_companies.company_type_id AS company_type_id, movie_companies.note AS note, movie_info.info AS info, movie_info.info_type_id AS info_type_id
 from movie_keyword,movie_info,movie_companies,complete_cast,keyword
 where (complete_cast.movie_id = movie_keyword.movie_id) And (movie_companies.movie_id = movie_keyword.movie_id) And (movie_info.movie_id = movie_keyword.movie_id) And (movie_keyword.keyword_id = keyword.id) And (movie_info.info in ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American')) And (movie_companies.note not like '%(USA)%')  And  (movie_companies.note like '%(200%)%') And (keyword.keyword in ('murder', 'murder-in-title', 'blood', 'violence'))