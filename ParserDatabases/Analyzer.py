class LogAnalyzer:
    def __init__(self, db_connector):
        self.db_connector = db_connector
    
    def get_ip_user_agent_statistics(self, n, db_type):
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
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
    
        result = self.db_connector.execute_query(query)
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
            query = """
                db.log_data.aggregate([
                    {{
                        $group: {{
                            _id: {{
                                year: {{ $year: "$timestamp" }},
                                month: {{ $month: "$timestamp" }},
                                day: {{ $dayOfMonth: "$timestamp" }},
                                hour: {{ $hour: "$timestamp" }},
                                minute: {{ $minute: "$timestamp" }}
                            }},
                            frequency: {{ $sum: 1 }}
                        }}
                    }},
                    {{
                        $sort: {{ "_id.year": 1, "_id.month": 1, "_id.day": 1, "_id.hour": 1, "_id.minute": 1 }}
                    }}
                ])
            """
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
            query = f"""
                db.log_data.aggregate([
                    {{
                        $group: {{
                            _id: "$user_agent",
                            frequency: {{ $sum: 1 }}
                        }}
                    }},
                    {{
                        $sort: {{ frequency: -1 }}
                    }},
                    {{
                        $limit: {N}
                    }}
                ])
            """
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
            query = """
                db.log_data.aggregate([
                    {{
                        $match: {{
                            status_code: /^5/
                            timestamp: {{
                                $gte: ISODate(new Date() - {dT} * 60000)
                            }}
                        }}
                    }},
                    {{
                        $group: {{
                            _id: "$status_code",
                            frequency: {{ $sum: 1 }}
                        }}
                    }}
                ])
            """
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query, (dT,))
        return result


    def get_longest_shortest_requests(self, limit, order_by, db_type):
        if order_by == "longest":
            order_by_clause = "DESC"
        elif order_by == "shortest":
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
            query = f"""
                db.log_data.find().sort({{"time_taken": {"$order_by_clause": 1}}}).limit({limit})
            """
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
            query = f"""
                db.log_data.aggregate([
                    {{
                        $match: {{
                            request: /^GET /
                        }}
                    }},
                    {{
                        $group: {{
                            _id: {{
                                $substr: ["$request", 0, {slash_count+5}]
                            }},
                            frequency: {{ $sum: 1 }}
                        }}
                    }},
                    {{
                        $sort: {{ frequency: -1 }}
                    }},
                    {{
                        $limit: {N}
                    }}
                ])
            """
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
            query = """
                db.log_data.aggregate([
                    {{
                        $match: {{
                            BALANCER_WORKER_NAME: {{ $exists: true }}
                        }}
                    }},
                    {{
                        $group: {{
                            _id: "$BALANCER_WORKER_NAME",
                            request_count: {{ $sum: 1 }},
                            average_time: {{ $avg: "$timestamp" }}
                        }}
                    }}
                ])
            """
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
            query = f"""
                db.log_data.aggregate([
                    {{
                        $match: {{
                            Referer: {{ $exists: true }}
                        }}
                    }},
                    {{
                        $group: {{
                            _id: {{
                                $arrayElemAt: [
                                    {{
                                        $split: ["$Referer", "/"]
                                    }},
                                    2
                                ]
                            }},
                            conversion_count: {{ $sum: 1 }}
                        }}
                    }},
                    {{
                        $sort: {{ "conversion_count": -1 }}
                    }}
                ])
            """
        elif db_type == "Redis":
            raise NotImplementedError("Redis database not supported for this query.")
        else:
            raise ValueError("Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
        result = self.db_connector.execute_query(query)
    
#    def get_upstream_requests(self, interval):
#        query = """
#            SELECT COUNT(*) AS upstream_request_count, AVG(time_taken) AS average_time
#            FROM log_data
#            WHERE `timestamp` >= NOW() - INTERVAL %s
#                AND `BALANCER_WORKER_NAME` IS NOT NULL
#        """
#        result = self.db_connector.execute_query(query, (interval,))
#        return result
#    
#    def find_most_active_periods(self, N):
#        query = """
#            SELECT CONCAT(DATE_FORMAT(`timestamp`, '%%Y-%%m-%%d %%H:'), LPAD((MINUTE(`timestamp`) DIV %s) * %s, 2, '0')) AS period,
#            COUNT(*) AS request_count
#            FROM log_data
#            WHERE `timestamp` >= DATE_SUB(NOW(), INTERVAL %s)
#            GROUP BY period
#            ORDER BY request_count DESC
#            LIMIT %s
#        """
#        result = self.db_connector.execute_query(query, (N, N, N, N))
        return result