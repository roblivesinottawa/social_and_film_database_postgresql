import psycopg2
import sqlalchemy
from sqlalchemy import create_engine

class Game:
    def __init__(self):
        self.db_name = 'game'
        self.db_user = 'rob_the_programmer'
        self.db_password = input('Enter your password: ')
        self.db_host = 'localhost'
        self.db_port = '5432'

    def connect(self):
        '''Connect to the PostgreSQL database server'''
        self.conn = None
        try:
            params = {
                'database': self.db_name,
                'user': self.db_user,
                'password': self.db_password,
                'host': self.db_host,
                'port': self.db_port
            }
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params)
            self.cur = self.conn.cursor()
            print('PostgreSQL database version:')
            self.cur.execute('SELECT version()')
            db_version = self.cur.fetchone()
            print(db_version)
            self.cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def create_tables(self):
        '''Create tables in the PostgreSQL database'''
        self.queries = (
            '''CREATE TABLE IF NOT EXISTS game (
                id SERIAL PRIMARY KEY,
                age SMALLINT NOT NULL
            );'''
        )
        self.conn = None
        try:
            params = {
                'database': self.db_name,
                'user': self.db_user,
                'password': self.db_password,
                'host': self.db_host,
                'port': self.db_port
            }
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params)
            self.cur = self.conn.cursor()

            for query in self.queries:
                self.cur.execute(query)
            self.conn.commit()
            self.cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print('Database connection closed.')



db = Game()
db.connect()
# db.create_tables()
