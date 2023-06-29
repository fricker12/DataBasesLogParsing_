import pymysql
import psycopg2
import sqlite3
import h2
from pymongo import MongoClient
import redis

class DatabaseConnector:
    def __init__(self, database, host=None, port=None, username=None, password=None, db_name=None, mongodb_uri=None):
        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.connection = None
        self.mongodb_uri = mongodb_uri

    def connect(self):
        if self.database == "MySQL":
            self.connection = self.connect_mysql()
        elif self.database == "PostgreSQL":
            self.connection = self.connect_postgresql()
        elif self.database == "SQLite":
            self.connection = self.connect_sqlite()
        elif self.database == "H2":
            self.connection = self.connect_h2()
        elif self.database == "MongoDB":
            self.connection = self.connect_mongodb()
        elif self.database == "Redis":
            self.connection = self.connect_redis()

    def connect_mysql(self):
        connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            charset='utf8'
        )
        # Подключение к созданной или существующей базе данных
        connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.db_name,
            charset='utf8'
        )

        return connection

    def connect_postgresql(self):
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            dbname=self.db_name
        )
        return connection

    def connect_sqlite(self):
        connection = sqlite3.connect(self.db_name)
        return connection

    def connect_h2(self):
        connection = h2.H2Database(":memory:")
        return connection

    def connect_mongodb(self):
        client = MongoClient(self.mongodb_uri)
        connection = client[self.db_name]
        return connection

    def connect_redis(self):
        connection = redis.Redis(host=self.host, port=self.port, password=self.password, db=self.db_name)
        return connection

    # Запрос к базе данных
    def execute_query(self, query, params=None):
        if self.database == "MongoDB":
            # Выполнение запроса к базе данных MongoDB
            #print(f'Запрос {query} соединение {self.connection}')
            result = self.connection['log_data'].aggregate(query)
        else:
        # Выполнение запроса к реляционной базе данных
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result