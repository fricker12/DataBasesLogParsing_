import time
import re
import abc

class DatabaseStrategy(abc.ABC):
    def __init__(self,cursor,connection):
        self.cursor = cursor
        self.connection = connection
    @abc.abstractmethod
    def create_table(self):
        pass
    @abc.abstractmethod
    def insert_data(self, values):
        pass
    @abc.abstractmethod
    def export_data(self, log_file):
        pass

class MySQLStrategy(DatabaseStrategy):
    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS log_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ip_address VARCHAR(255),
                forwarded_for VARCHAR(255),
                timestamp VARCHAR(255),
                request LONGTEXT,
                status_code INT,
                response_size INT,
                time_taken INT,
                referer LONGTEXT,
                user_agent LONGTEXT,
                balancer_worker_name VARCHAR(255)
            )
        """
        self.cursor.execute(create_table_query)

    def insert_data(self, values):
        sql = "INSERT INTO log_data (ip_address, forwarded_for, timestamp, request, status_code, response_size, time_taken, referer, user_agent, balancer_worker_name) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.cursor.execute(sql, values)

    def export_data(self, log_file):
        select_query = "SELECT * FROM log_data"
        self.cursor.execute(select_query)
        log_data = self.cursor.fetchall()

        with open(log_file, 'w') as file:
            for data in log_data:
                line = f"{data[1]} ({data[2]}) - - [{data[3]}] \"{data[4]}\" {data[5]} {data[6]} {data[7]} {data[10]} \"{data[8]}\" \"{data[9]}\"\n"
                file.write(line)
                
class H2DatabaseStrategy(DatabaseStrategy):
    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS log_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ip_address VARCHAR(255),
                forwarded_for VARCHAR(255),
                timestamp VARCHAR(255),
                request LONGTEXT,
                status_code INT,
                response_size INT,
                time_taken INT,
                referer LONGTEXT,
                user_agent LONGTEXT,
                balancer_worker_name VARCHAR(255)
            )
        """
        self.cursor.execute(create_table_query)

    def insert_data(self, values):
        sql = "INSERT INTO log_data (ip_address, forwarded_for, timestamp, request, status_code, response_size, time_taken, referer, user_agent, balancer_worker_name) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        self.cursor.execute(sql, values)

    def export_data(self, log_file):
        select_query = "SELECT * FROM log_data"
        self.cursor.execute(select_query)
        log_data = self.cursor.fetchall()

        with open(log_file, 'w') as file:
            for data in log_data:
                line = f"{data[1]} ({data[2]}) - - [{data[3]}] \"{data[4]}\" {data[5]} {data[6]} {data[7]} {data[10]} \"{data[8]}\" \"{data[9]}\"\n"
                file.write(line)

class RedisStrategy(DatabaseStrategy):
    # В Redis не нужно создавать таблицы, так как данные хранятся в ключах и значениях
    def insert_data(self, values):
        key = self._generate_key()
        data = {
            "ip_address": values[0],
            "forwarded_for": values[1],
            "timestamp": values[2],
            "request": values[3],
            "status_code": values[4],
            "response_size": values[5],
            "time_taken": values[6],
            "referer": values[7],
            "user_agent": values[8],
            "balancer_worker_name": values[9]
        }
        self.cursor.set(key, data)

    def export_data(self, log_file):
        log_data = self.cursor.keys()

        with open(log_file, 'w') as file:
            for key in log_data:
                data = self.cursor.get(key)
                line = f"{data['ip_address']} ({data['forwarded_for']}) - - [{data['timestamp']}] \"{data['request']}\" {data['status_code']} {data['response_size']} {data['time_taken']} {data['balancer_worker_name']} \"{data['referer']}\" \"{data['user_agent']}\"\n"
                file.write(line)

    def _generate_key(self):
        # Генерация уникального ключа для Redis (может быть определено по своему)
        return str(time.time())

class PostgreSQLStrategy(DatabaseStrategy):
    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS log_data (
                id SERIAL PRIMARY KEY,
                ip_address VARCHAR(255),
                forwarded_for VARCHAR(255),
                timestamp VARCHAR(255),
                request TEXT,
                status_code INT,
                response_size INT,
                time_taken INT,
                referer TEXT,
                user_agent TEXT,
                balancer_worker_name VARCHAR(255)
            )
        """
        self.cursor.execute(create_table_query)

    def insert_data(self, values):
        sql = "INSERT INTO log_data (ip_address, forwarded_for, timestamp, request, status_code, response_size, time_taken, referer, user_agent, balancer_worker_name) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.cursor.execute(sql, values)

    def export_data(self, log_file):
        select_query = "SELECT * FROM log_data"
        self.cursor.execute(select_query)
        log_data = self.cursor.fetchall()

        with open(log_file, 'w') as file:
            for data in log_data:
                line = f"{data[1]} ({data[2]}) - - [{data[3]}] \"{data[4]}\" {data[5]} {data[6]} {data[7]} {data[10]} \"{data[8]}\" \"{data[9]}\"\n"
                file.write(line)

class SQLiteStrategy(DatabaseStrategy):
    def create_table(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS log_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_address TEXT,
                forwarded_for TEXT,
                timestamp TEXT,
                request TEXT,
                status_code INTEGER,
                response_size INTEGER,
                time_taken INTEGER,
                referer TEXT,
                user_agent TEXT,
                balancer_worker_name TEXT
            )
        """
        self.cursor.execute(create_table_query)

    def insert_data(self, values):
        sql = "INSERT INTO log_data (ip_address, forwarded_for, timestamp, request, status_code, response_size, time_taken, referer, user_agent, balancer_worker_name) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        self.cursor.execute(sql, values)

    def export_data(self, log_file):
        select_query = "SELECT * FROM log_data"
        self.cursor.execute(select_query)
        log_data = self.cursor.fetchall()

        with open(log_file, 'w') as file:
            for data in log_data:
                line = f"{data[1]} ({data[2]}) - - [{data[3]}] \"{data[4]}\" {data[5]} {data[6]} {data[7]} {data[10]} \"{data[8]}\" \"{data[9]}\"\n"
                file.write(line)

class MongoDBStrategy(DatabaseStrategy):
    def __init__(self, collection):
        self.collection = collection
    def create_table(self):
        # В MongoDB не нужно явно создавать таблицы, они создаются при первой вставке документа
        pass
    def insert_data(self, values):
        data = {
            "ip_address": values[0],
            "forwarded_for": values[1],
            "timestamp": values[2],
            "request": values[3],
            "status_code": values[4],
            "response_size": values[5],
            "time_taken": values[6],
            "referer": values[7],
            "user_agent": values[8],
            "balancer_worker_name": values[9]
        }
        self.collection.insert_one(data)

    def export_data(self, log_file):
        log_data = self.collection.find()

        with open(log_file, 'w') as file:
            for data in log_data:
                line = f"{data['ip_address']} ({data['forwarded_for']}) - - [{data['timestamp']}] \"{data['request']}\" {data['status_code']} {data['response_size']} {data['time_taken']} {data['balancer_worker_name']} \"{data['referer']}\" \"{data['user_agent']}\"\n"
                file.write(line)

class LogDataManager:
    def __init__(self, db_connector, database_type):
        self.db_connector = db_connector
        self.database_type = database_type

    def import_log_data(self, log_file):
        self.db_connector.connect()
        start_time = time.time()

        strategy = self._get_strategy()
        strategy.create_table()

        with open(log_file, 'r') as file:
            log_data = file.readlines()

        for line in log_data:
            match = re.match(self._get_regex(), line)
            if match:
                values = match.groups()
                strategy.insert_data(values)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Импорт лога в базу данных выполнен за {execution_time} секунд.")

    def export_log_data(self, log_file):
        self.db_connector.connect()
        start_time = time.time()

        strategy = self._get_strategy()
        strategy.export_data(log_file)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Экспорт лога из базы данных выполнен за {execution_time} секунд.")

    def _get_strategy(self):
        if self.database_type == "MySQL":
            return MySQLStrategy(cursor=self.db_connector.connection.cursor())
        elif self.database_type == "PostgreSQL":
            return PostgreSQLStrategy(cursor=self.db_connector.connection.cursor())
        elif self.database_type == "SQLite":
            return SQLiteStrategy(cursor=self.db_connector.connection.cursor())
        elif self.database_type == "MongoDB":
            return MongoDBStrategy(collection=self.db_connector.connection.log_data)
        elif self.database_type == "H2":
            return H2DatabaseStrategy(cursor=self.db_connector.connection.cursor())
        elif self.database_type == "Redis":
            return RedisStrategy(cursor=self.db_connector.connection)
        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")

    def _get_regex(self):
        # Ваше регулярное выражение для извлечения данных из строки лога
        regex = r'^(?P<ip_address>\S+) \((?P<forwarded_for>\S+)\) - - \[(?P<timestamp>[\w:/]+\s[+\-]\d{4})\] "(?P<request>[A-Z]+ \S+ \S+)" (?P<status_code>\d+) (?P<response_size>\d+) (?P<time_taken>\d+) (?P<balancer_worker_name>\d+) "(?P<Referer>[^"]*)" "(?P<user_agent>[^"]*)"'
        return regex
