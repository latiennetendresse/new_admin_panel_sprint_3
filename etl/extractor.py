import logging
import os

from dotenv import load_dotenv
from psycopg2 import connect, sql
from psycopg2.extras import DictCursor

from decorator import backoff

load_dotenv()


class PGExtractor:
    BASE_XXXXL_QUERY = f"""with changed_rows_in_film_work as (SELECT fw.id, fw.rating, fw.title, fw.description,
     fw.updated_at, g.name genre, pfw.role, p.id person_id, p.full_name person_full_name
     FROM content.film_work fw
     LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
     LEFT JOIN content.person p ON p.id = pfw.person_id
     LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
     LEFT JOIN content.genre g ON g.id = gfw.genre_id),

     changed_rows_in_genre as (SELECT fw.id, fw.rating, fw.title, fw.description,
     fw.updated_at, g.name genre, pfw.role, p.id person_id, p.full_name person_full_name
     FROM content.genre g
     JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
     JOIN content.film_work fw ON fw.id = gfw.film_work_id
     LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
     LEFT JOIN content.person p ON p.id = pfw.person_id),

     changed_rows_in_person as (SELECT fw.id, fw.rating, fw.title, fw.description,
     fw.updated_at, g.name genre, pfw.role, p.id person_id, p.full_name person_full_name
     FROM content.person p
     JOIN content.person_film_work pfw ON pfw.person_id = p.id
     JOIN content.film_work fw ON fw.id = pfw.film_work_id
     LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
     LEFT JOIN content.genre g ON g.id = gfw.genre_id),

     all_changed_rows as (SELECT * FROM changed_rows_in_film_work UNION ALL
     SELECT * FROM changed_rows_in_genre UNION ALL
     SELECT * FROM changed_rows_in_person),

     all_changed_rows_deduplicated AS (
     SELECT id, rating, title, description, max(updated_at) as updated_at, genre, role, person_id, person_full_name
     FROM all_changed_rows GROUP BY id,rating, title, description, genre, role, person_id, person_full_name),
     aggregate_genres_and_roles_to_json as (SELECT id, rating, title, description, updated_at, array_agg(DISTINCT genre) as genre,
     COALESCE(json_agg(DISTINCT jsonb_build_object('person_role', role,
                                                    'person_id', person_id,
                                                    'person_name', person_full_name))
    FILTER (WHERE person_id is not null), '[]') as persons FROM all_changed_rows_deduplicated GROUP BY id, rating,
     title, description, updated_at)
      SELECT id, rating, genre,
     title, description, updated_at, persons FROM aggregate_genres_and_roles_to_json"""

    def __init__(self):
        self.dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'),
                    'password': os.environ.get('DB_PASSWORD'),
                    'host': os.environ.get('DB_HOST'), 'port': os.environ.get('DB_PORT')}
        self.conn = self.pg_connect()

    @backoff(start_sleep_time=0.5)
    def pg_connect(self):
        return connect(**self.dsl, cursor_factory=DictCursor)

    @backoff(start_sleep_time=0.5)
    def get_updated_films(self, last_update: str, limit: int, offset: int):
        logging.info('Получение пачки фильмов старше даты состояния')
        try:
            supplemented_query = self.BASE_XXXXL_QUERY + ' WHERE updated_at > %s ORDER BY updated_at LIMIT %s OFFSET %s;'
            with self.conn as conn, conn.cursor() as cursor:
                cursor.execute(supplemented_query, (last_update, limit, offset))
                rows = cursor.fetchall()
            logging.info('Пачка фильмов успешно получена')
            return rows
        except Exception as error:
            logging.error('Ошибка при получении пачки фильмов', error)

    @backoff(start_sleep_time=0.5)
    def get_all_films(self, limit: int, offset: int):
        logging.info('Получение первичной пачки фильмов')
        try:
            supplemented_query = self.BASE_XXXXL_QUERY + ' ORDER BY updated_at LIMIT %s OFFSET %s;'
            with self.conn as conn, conn.cursor() as cursor:
                cursor.execute(supplemented_query, (limit, offset))
                rows = cursor.fetchall()
            logging.info('Первичная пачка фильмов успешно получена')
            return rows
        except Exception as error:
            logging.error('Ошибка при получении первичной пачки фильмов')
