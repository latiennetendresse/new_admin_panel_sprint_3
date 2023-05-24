import logging

from psycopg2 import connect
from psycopg2.extras import DictCursor

from decorator import backoff
from settings import PGSettings


class PGExtractor:
    BASE_XXXXL_QUERY = f"""WITH changed_rows_in_film_work AS (SELECT fw.id, fw.rating, fw.title, fw.description,
     fw.modified, g.name genre, pfw.role, p.id person_id, p.full_name person_full_name
     FROM content.film_work fw
     LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
     LEFT JOIN content.person p ON p.id = pfw.person_id
     LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
     LEFT JOIN content.genre g ON g.id = gfw.genre_id),

     changed_rows_in_genre as (SELECT fw.id, fw.rating, fw.title, fw.description,
     fw.modified, g.name genre, pfw.role, p.id person_id, p.full_name person_full_name
     FROM content.genre g
     JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
     JOIN content.film_work fw ON fw.id = gfw.film_work_id
     LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
     LEFT JOIN content.person p ON p.id = pfw.person_id),

     changed_rows_in_person as (SELECT fw.id, fw.rating, fw.title, fw.description,
     fw.modified, g.name genre, pfw.role, p.id person_id, p.full_name person_full_name
     FROM content.person p
     JOIN content.person_film_work pfw ON pfw.person_id = p.id
     JOIN content.film_work fw ON fw.id = pfw.film_work_id
     LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
     LEFT JOIN content.genre g ON g.id = gfw.genre_id),

     all_changed_rows as (SELECT * FROM changed_rows_in_film_work UNION ALL
     SELECT * FROM changed_rows_in_genre UNION ALL
     SELECT * FROM changed_rows_in_person),

     all_changed_rows_deduplicated AS (
     SELECT id, rating, title, description, max(modified) as modified, genre, role, person_id, person_full_name
     FROM all_changed_rows GROUP BY id, rating, title, description, genre, role, person_id, person_full_name),
     aggregate_genres_and_roles_to_json as (SELECT id, rating, title, description, modified, array_agg(DISTINCT genre) as genre,
     COALESCE(json_agg(DISTINCT jsonb_build_object('person_role', role,
                                                    'person_id', person_id,
                                                    'person_name', person_full_name))
    FILTER (WHERE person_id is not null), '[]') as persons FROM all_changed_rows_deduplicated GROUP BY id, rating,
     title, description, modified)
      SELECT id, rating, genre,
     title, description, modified, persons FROM aggregate_genres_and_roles_to_json"""

    def __init__(self):
        pg_settings = PGSettings()
        self.dsl = {'dbname': pg_settings.DB_NAME, 'user': pg_settings.DB_USER,
                    'password': pg_settings.DB_PASSWORD,
                    'host': pg_settings.DB_HOST, 'port': pg_settings.DB_PORT}
        self.conn = self.pg_connect()

    @backoff(start_sleep_time=0.5)
    def pg_connect(self):
        return connect(**self.dsl, cursor_factory=DictCursor)

    @backoff(start_sleep_time=0.5)
    def get_updated_films(self, last_update: str, limit: int, offset: int):
        logging.info('Получение пачки фильмов старше даты состояния')
        supplemented_query = self.BASE_XXXXL_QUERY + ' WHERE modified > %s ORDER BY modified LIMIT %s OFFSET %s;'
        with self.conn as conn, conn.cursor() as cursor:
            cursor.execute(supplemented_query, (last_update, limit, offset))
            rows = cursor.fetchall()
        logging.info('Пачка фильмов успешно получена')
        return rows

    @backoff(start_sleep_time=0.5)
    def get_all_films(self, limit: int, offset: int):
        logging.info('Получение первичной пачки фильмов')
        supplemented_query = self.BASE_XXXXL_QUERY + ' ORDER BY modified LIMIT %s OFFSET %s;'
        with self.conn as conn, conn.cursor() as cursor:
            cursor.execute(supplemented_query, (limit, offset))
            rows = cursor.fetchall()
        logging.info('Первичная пачка фильмов успешно получена')
        return rows
