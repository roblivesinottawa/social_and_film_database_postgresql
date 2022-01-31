import psycopg2
from config import config

def connect():
    '''Connect to the PostGresQL Database Server'''
    conn = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print(db_version)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def create_tables():
    '''creates tables in the PostGreSQL Database'''
    queries = (
        '''CREATE TABLE IF NOT EXISTS account (
            id SERIAL PRIMARY KEY,
            email VARCHAR(128) NOT NULL DEFAULT NOW(),
            updated_at DATE NOT NULL DEFAULT NOW());''',

        '''CREATE TABLE IF NOT EXISTS post (
            id SERIAL PRIMARY KEY,
            title VARCHAR(128) UNIQUE NOT NULL,
            content TEXT,
            account_id INTEGER REFERENCES account(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );''',
        '''CREATE TABLE IF NOT EXISTS comments (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            account_id INTEGER REFERENCES account(id) ON DELETE CASCADE,
            post_id INTEGER REFERENCES post(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );''',
        '''CREATE TABLE IF NOT EXISTS favorites (
            id SERIAL PRIMARY KEY,
            post_id INTEGER REFERENCES post(id) ON DELETE CASCADE,
            account_id INTEGER REFERENCES account(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(post_id, account_id)
        );'''


)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for query in queries:
            cur.execute(query)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__=='__main__':
    connect()
    create_tables()