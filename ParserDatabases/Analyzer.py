from datetime import datetime
import time
import json
class LogAnalyzer:
    def __init__(self, db_connector):
        self.db_connector = db_connector
    
    def get_ip_user_agent_statistics(self, n, db_type):
        start_time = time.time()
        query = ""
        if db_type == "MySQL":
            query = f"""
                SELECT ip_address, user_agent, COUNT(*) as count
                FROM log_data
                GROUP BY ip_address, user_agent
                ORDER BY count DESC
                LIMIT {n}
            """
        elif db_type == "PostgreSQL":
            query = f"""
                SELECT ip_address, user_agent, COUNT(*) as count
                FROM log_data
                GROUP BY ip_address, user_agent
                ORDER BY count DESC
                LIMIT {n}
            """
        elif db_type == "SQLite":
            query = f"""
                SELECT ip_address, user_agent, COUNT(*) as count
                FROM log_data
                GROUP BY ip_address, user_agent
                ORDER BY count DESC
                LIMIT {n}
            """
        elif db_type == "H2":
            query = f"""
                SELECT ip_address, user_agent, COUNT(*) as count
                FROM log_data
                GROUP BY ip_address, user_agent
                ORDER BY count DESC
                LIMIT {n}
            """
        elif db_type == "MongoDB":
            query = [
                {"$group": {"_id": {"ip_address": "$ip_address", "user_agent": "$user_agent"}, "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": n}
                ]
        elif db_type == "Redis": 
            # Get all keys from the Redis database
            keys = self.db_connector.connection.keys("*")
            # Initialize a dictionary to store the counts for each IP address and user agent combination
            counts = {}
            # Iterate over the keys
            for key in keys:
                # Get the JSON data for the current key
                json_data = self.db_connector.connection.get(key)
                # Load the JSON data into a dictionary
                data = json.loads(json_data)
                # Get the IP address and user agent from the data
                ip_address = data["ip_address"]
                user_agent = data["user_agent"]
                # Create a tuple representing the IP address and user agent combination
                key = (ip_address, user_agent)
                # Increment the count for this IP address and user agent combination
                counts[key] = counts.get(key, 0) + 1
                # Convert the counts dictionary into a list of tuples
            data = list(counts.items())
            # Sort the data by count in descending order
            data.sort(key=lambda x: x[1], reverse=True)
            # Get the top n results
            result = data[:n]
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Запрос выполнился за {execution_time} секунд.")
            return result

        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        if db_type != "Redis":
            result = self.db_connector.execute_query(query)
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Запрос выполнился за {execution_time} секунд.")
            return result

    def get_query_frequency(self, dT, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = """
                SELECT DATE_FORMAT(timestamp, '%%Y-%%m-%%d %%H:%%i') AS interval_start, COUNT(*) AS frequency
                FROM log_data
                GROUP BY interval_start
                ORDER BY interval_start
            """
        elif db_type == "MongoDB":
            query = [
                        {
                            "$group": {
                                "_id": {
                                    "year": {"$year": "$timestamp"},
                                    "month": {"$month": "$timestamp"},
                                    "day": {"$dayOfMonth": "$timestamp"},
                                    "hour": {"$hour": "$timestamp"},
                                    "minute": {"$minute": "$timestamp"}
                                },
                                "frequency": {"$sum": 1}
                            }
                        },
                        {
                            "$sort": {
                                "_id.year": 1,
                                "_id.month": 1,
                                "_id.day": 1,
                                "_id.hour": 1,
                                "_id.minute": 1
                            }
                        }
                    ]
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query)
        return result

    def get_top_user_agents(self, N, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = """
                SELECT user_agent, COUNT(*) AS frequency
                FROM log_data
                GROUP BY user_agent
                ORDER BY frequency DESC
                LIMIT %s
            """
        elif db_type == "MongoDB":
            query = [
                    {
                        "$group": {
                            "_id": "$user_agent",
                            "frequency": {"$sum": 1}
                        }
                    },
                    {
                        "$sort": {"frequency": -1}
                    },
                    {
                        "$limit": N
                    }
                ]
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query, (N,))
        return result

    def get_status_code_statistics(self, dT, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = """
                SELECT status_code, COUNT(*) AS frequency
                FROM log_data
                WHERE status_code LIKE '5%%'
                AND timestamp >= NOW() - INTERVAL %s MINUTE
                GROUP BY status_code
            """
        elif db_type == "MongoDB":
            query = [
                    {
                        "$match": {
                            "status_code": {"$regex": "^5"},
                            "timestamp": {
                                "$gte": datetime.datetime.now() - datetime.timedelta(minutes=dT)
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": "$status_code",
                            "frequency": {"$sum": 1}
                        }
                    }
                ]
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query, (dT,))
        return result


    def get_longest_shortest_requests(self, limit, order_by, db_type):
        if order_by == "longest":
            if db_type == "MongoDB":
                order_by_clause = -1
            else:
                order_by_clause = "DESC"        
        elif order_by == "shortest":
            if db_type == "MongoDB":
                order_by_clause = 1
            else:
                order_by_clause = "ASC"        
        else:
            raise ValueError("Invalid order_by value. Must be 'longest' or 'shortest'.")
        
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = f"""
                SELECT request, time_taken
                FROM log_data
                ORDER BY time_taken {order_by_clause}
                LIMIT %s
            """
        elif db_type == "MongoDB":
            query = [
                    {
                        "$project": {
                            "request": 1,
                            "time_taken": 1
                        }
                    },
                    {
                        "$sort": {"time_taken": order_by_clause}
                    },
                    {
                        "$limit": limit
                    }
                ]
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query, (limit,))
        return result

    def get_common_requests(self, N, slash_count, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = f"""
                SELECT SUBSTRING_INDEX(request, ' ', {slash_count+1}) AS request_pattern, COUNT(*) AS frequency
                FROM log_data
                WHERE request LIKE 'GET %%'
                GROUP BY request_pattern
                ORDER BY frequency DESC
                LIMIT %s
            """
        elif db_type == "MongoDB":
            query = [
                        {
                            "$match": {
                                "request": {"$regex": "^GET "}
                            }
                        },
                        {
                            "$project": {
                                "request_pattern": {"$arrayElemAt": [{"$split": ["$request", " "]}, slash_count]}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$request_pattern",
                                "frequency": {"$sum": 1}
                            }
                        },
                        {
                            "$sort": {"frequency": -1}
                        },
                        {
                            "$limit": N
                        }
                    ]
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query, (N,))
        return result

    def get_upstream_requests_WORKER(self, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = """
                SELECT BALANCER_WORKER_NAME, COUNT(*) AS request_count, AVG(timestamp) AS average_time
                FROM log_data
                WHERE BALANCER_WORKER_NAME IS NOT NULL
                GROUP BY BALANCER_WORKER_NAME
            """
        elif db_type == "MongoDB":
            query = [
                    {
                        "$match": {
                            "BALANCER_WORKER_NAME": {"$exists": True}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$BALANCER_WORKER_NAME",
                            "request_count": {"$sum": 1},
                            "average_time": {"$avg": "$timestamp"}
                        }
                    }
                ]
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query)
        return result

    def get_conversion_statistics(self, sort_by, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = """
                SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(Referer, '/', 3), '/', -1) AS domain,
                COUNT(*) AS conversion_count
                FROM log_data
                WHERE Referer IS NOT NULL
                GROUP BY domain
                ORDER BY {} DESC
            """.format(sort_by)
        elif db_type == "MongoDB":
            query = [
                    {
                        "$match": {
                            "Referer": {"$exists": True}
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "$arrayElemAt": [
                                    {"$split": ["$Referer", "/"]},
                                    2
                                ]
                            },
                            "conversion_count": {"$sum": 1}
                        }
                    },
                    {
                        "$sort": {"conversion_count": -1}
                    }
                ]
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query)
        return result
    
    def get_upstream_requests(self, interval, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = """
            SELECT COUNT(*) AS upstream_request_count, AVG(time_taken) AS average_time
            FROM log_data
            WHERE `timestamp` >= NOW() - INTERVAL %s
                AND `BALANCER_WORKER_NAME` IS NOT NULL
            """
        elif db_type == "MongoDB":
            query = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.datetime.now() - datetime.timedelta(minutes=interval)
                        }
                    }
                },
                {
                    "$match": {
                        "BALANCER_WORKER_NAME": {"$ne": None}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "upstream_request_count": {"$sum": 1},
                        "average_time": {"$avg": "$time_taken"}
                    }
                }
            ]
            
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
            
        result = self.db_connector.execute_query(query, (interval,))
        return result
    
    def find_most_active_periods(self, N, db_type):
        query = ""
        if db_type == "MySQL" or db_type == "PostgreSQL" or db_type == "SQLite" or db_type == "H2":
            query = """
                SELECT CONCAT(DATE_FORMAT(`timestamp`, '%%Y-%%m-%%d %%H:'), LPAD((MINUTE(`timestamp`) DIV %s) * %s, 2, '0')) AS period,
                COUNT(*) AS request_count
                FROM log_data
                WHERE `timestamp` >= DATE_SUB(NOW(), INTERVAL %s)
                GROUP BY period
                ORDER BY request_count DESC
                LIMIT %s
            """
        elif db_type == "MongoDB":
            query = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": {
                                "$subtract": ["$$NOW", { "$multiply": [N, 60000] }]
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "$concat": [
                                { "$dateToString": { "format": "%Y-%m-%d %H:", "date": "$timestamp" } },
                                { "$substr": [{ "$multiply": [{ "$minute": "$timestamp" }, N] }, 0, -1] }
                            ]
                        },
                        "request_count": { "$sum": 1 }
                    }
                },
                { "$sort": { "request_count": -1 } },
            ]
               
        result = self.db_connector.execute_query(query, (N, N, N, N))
        return result