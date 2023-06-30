import argparse
import logging 
from ParserDatabases import Connector
from ParserDatabases import Analyzer
from ParserDatabases import Data

def main():
    parser = argparse.ArgumentParser(description="Log Analyzer")
    parser.add_argument("--database", type=str, required=True, choices=["MySQL", "PostgreSQL", "SQLite", "H2", "MongoDB", "Redis"], help="Database type")
    parser.add_argument("--host", type=str, help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--username", type=str, help="Database username")
    parser.add_argument("--password", type=str, help="Database password")
    parser.add_argument("--db_index", type=int, help="Database index for Redis")
    parser.add_argument("--db_name", type=str, help="Database name")
    parser.add_argument("--log_file", type=str, required=True, help="Path to the log file")
    parser.add_argument("--mongodb_uri", type=str, help="MongoDB connection URI")
    parser.add_argument('--export', metavar='filename', help='Export log data to a file')
    # Добавляем аргументы
    parser.add_argument('--analyze', choices=['ip_user_agent', 'query_frequency', 'top_user_agents', 'status_code', 'longest_shortest_requests', 'common_requests', 'upstream_requests_worker', 'conversion_statistics', 'upstream_requests', 'most_active_periods'], help='Specify the analysis function to run')
    args = parser.parse_args()
    
    if args.database == "MongoDB" and args.mongodb_uri:
        db_connector = Connector.DatabaseConnector(
            args.database,
            db_name=args.db_name,
            mongodb_uri=args.mongodb_uri
        )
    elif args.database in ["MySQL", "PostgreSQL", "SQLite", "H2"] and args.host and args.port and args.username and args.password:
        db_connector = Connector.DatabaseConnector(
            args.database,
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            db_name=args.db_name
        )
    elif args.database == "Redis":
        db_connector = Connector.DatabaseConnector(
            args.database,
            host=args.host,
            port=args.port,
            db_index=args.db_index
        )
        
    else:
        print("Error: Missing required arguments for the selected database type.")
        return
    
    db_connector.connect()
    log_data_manager = Data.LogDataManager(db_connector, database_type=args.database)
    log_data_manager.import_log_data(args.log_file)
    log_analyzer = Analyzer.LogAnalyzer(db_connector)
    
    if args.database in ["MySQL", "PostgreSQL", "SQLite", "H2"]:
        # Проверяем, какую функцию анализа нужно вызвать
        if args.analyze == 'ip_user_agent':
            # Вызываем функцию get_ip_user_agent_statistics и печатаем результат
            statistics = log_analyzer.get_ip_user_agent_statistics(5,args.database)
            print("IP Address\tUser Agent\tFrequency")
            for stats in statistics:
                print(f"{stats[0]}\t{stats[1]}\t{stats[2]}")
        elif args.analyze == 'query_frequency':
            # Вызываем функцию get_query_frequency и печатаем результат
            frequency = log_analyzer.get_query_frequency(args.database)
            print(frequency)
        elif args.analyze == 'top_user_agents':
            # Вызываем функцию get_top_user_agents и печатаем результат
            top_agents = log_analyzer.get_top_user_agents(args.database)
            print(top_agents)
        elif args.analyze == 'status_code':
            # Вызываем функцию get_status_code_statistics и печатаем результат
            status_code_stats = log_analyzer.get_status_code_statistics(args.database)
            print(status_code_stats)
        elif args.analyze == 'longest_shortest_requests':
            # Вызываем функцию get_longest_shortest_requests и печатаем результат
            longest_shortest = log_analyzer.get_longest_shortest_requests(args.database)
            print(longest_shortest)
        elif args.analyze == 'common_requests':
            # Вызываем функцию get_common_requests и печатаем результат
            common_requests = log_analyzer.get_common_requests(args.database)
            print(common_requests)
        elif args.analyze == 'upstream_requests_worker':
            # Вызываем функцию get_upstream_requests_WORKER и печатаем результат
            upstream_requests_worker = log_analyzer.get_upstream_requests_WORKER(args.database)
            print(upstream_requests_worker)
        elif args.analyze == 'conversion_statistics':
            # Вызываем функцию get_conversion_statistics и печатаем результат
            conversion_stats = log_analyzer.get_conversion_statistics(args.database)
            print(conversion_stats)
        elif args.analyze == 'upstream_requests':
            # Вызываем функцию get_upstream_requests и печатаем результат
            upstream_requests = log_analyzer.get_upstream_requests(args.database)
            print(upstream_requests)
        elif args.analyze == 'most_active_periods':
            # Вызываем функцию find_most_active_periods и печатаем результат
            active_periods = log_analyzer.find_most_active_periods(args.database)
            print(active_periods)
    
    elif args.database == "MongoDB":
        # Проверяем, какую функцию анализа нужно вызвать
        if args.analyze == 'ip_user_agent':
            # Вызываем функцию get_ip_user_agent_statistics и печатаем результат
            statistics = log_analyzer.get_ip_user_agent_statistics(5,args.database)
            print("IP Address\tUser Agent\tFrequency")
            for document in statistics:
                print(document['_id'])
                    
        elif args.analyze == 'query_frequency':
            # Вызываем функцию get_query_frequency и печатаем результат
            frequency = log_analyzer.get_query_frequency(args.database)
            print(frequency)
        elif args.analyze == 'top_user_agents':
            # Вызываем функцию get_top_user_agents и печатаем результат
            top_agents = log_analyzer.get_top_user_agents(args.database)
            print(top_agents)
        elif args.analyze == 'status_code':
            # Вызываем функцию get_status_code_statistics и печатаем результат
            status_code_stats = log_analyzer.get_status_code_statistics(args.database)
            print(status_code_stats)
        elif args.analyze == 'longest_shortest_requests':
            # Вызываем функцию get_longest_shortest_requests и печатаем результат
            longest_shortest = log_analyzer.get_longest_shortest_requests(args.database)
            print(longest_shortest)
        elif args.analyze == 'common_requests':
            # Вызываем функцию get_common_requests и печатаем результат
            common_requests = log_analyzer.get_common_requests(args.database)
            print(common_requests)
        elif args.analyze == 'upstream_requests_worker':
            # Вызываем функцию get_upstream_requests_WORKER и печатаем результат
            upstream_requests_worker = log_analyzer.get_upstream_requests_WORKER(args.database)
            print(upstream_requests_worker)
        elif args.analyze == 'conversion_statistics':
            # Вызываем функцию get_conversion_statistics и печатаем результат
            conversion_stats = log_analyzer.get_conversion_statistics(args.database)
            print(conversion_stats)
        elif args.analyze == 'upstream_requests':
            # Вызываем функцию get_upstream_requests и печатаем результат
            upstream_requests = log_analyzer.get_upstream_requests(args.database)
            print(upstream_requests)
        elif args.analyze == 'most_active_periods':
            # Вызываем функцию find_most_active_periods и печатаем результат
            active_periods = log_analyzer.find_most_active_periods(args.database)
            print(active_periods)
        
    elif args.database == "Redis":
        # Проверяем, какую функцию анализа нужно вызвать
        if args.analyze == 'ip_user_agent':
            # Вызываем функцию get_ip_user_agent_statistics и печатаем результат
            statistics = log_analyzer.get_ip_user_agent_statistics(5,args.database)
            print(f'Redis statistics {statistics}')
    else:
        print("Error: Invalid db_type value. Must be one of: MySQL, PostgreSQL, SQLite, H2, MongoDB.")
        
    if args.export:
        # Вызываем функцию экспорта лога
        log_data_manager.export_log_data(args.export)
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
