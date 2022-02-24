import psycopg2
from config import config
# import sqlalchemy
# from sqlalchemy import create_engine

class Game:
    def __init__(self):
        self.conn = None
        self.cur = None
        self.params = config()

    def connect(self):
        '''Connect to the PostGreSQL Database'''
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
            print('PostgreSQL database version:')
            self.cur.execute('SELECT version()')
            db_version = self.cur.fetchone()
            print(db_version)
            self.cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print('Database connection closed.')

    def create_tables(self):
        '''creates tables in the game database'''
        queries = (
            '''
                CREATE TABLE IF NOT EXISTS  account (
                userid SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(50) NOT NULL,
                email VARCHAR(250) UNIQUE NOT NULL,
                createdat TIMESTAMP NOT NULL,
                lastlogin TIMESTAMP);
            ''',
            '''
                CREATE TABLE IF NOT EXISTS job (
                jobid SERIAL PRIMARY KEY,
                jobname VARCHAR(200) UNIQUE NOT NULL);
            ''',
            '''
                CREATE TABLE IF NOT EXISTS accountjob (
                userid INTEGER REFERENCES account(userid),
                jobid INTEGER REFERENCES job(jobid),
                hiredate TIMESTAMP);
            '''
           ) 

        self.conn = None
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
            [self.cur.execute(query) for query in queries]
            self.conn.commit()
            print('tables created successfully.')
            self.cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            [self.conn.close() if self.conn is not None else None]
            

db = Game()
db.connect()
db.create_tables()

        