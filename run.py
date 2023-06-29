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
    parser.add_argument("--db_name", type=str, required=True, help="Database name")
    parser.add_argument("--log_file", type=str, required=True, help="Path to the log file")
    parser.add_argument("--mongodb_uri", type=str, help="MongoDB connection URI")
    args = parser.parse_args()
    
    if args.database == "MongoDB" and args.mongodb_uri:
        db_connector = Connector.DatabaseConnector(
            args.database,
            db_name=args.db_name,
            mongodb_uri=args.mongodb_uri
        )
    elif args.database == "MySQL" and args.host and args.port and args.username and args.password:
        db_connector = Connector.DatabaseConnector(
            args.database,
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            db_name=args.db_name
        )
    else:
        print("Error: Missing required arguments for the selected database type.")
        return
    
    db_connector.connect()
    log_data_manager = Data.LogDataManager(db_connector, database_type=args.database)
    log_data_manager.import_log_data(args.log_file)
    log_analyzer = Analyzer.LogAnalyzer(db_connector)
    #log_data_manager.export_log_data("exported_log_file.txt")
    
    ip_user_agent_stats = log_analyzer.get_ip_user_agent_statistics(5,args.database)
    print("IP Address\tUser Agent\tFrequency")
    for document in ip_user_agent_stats:
        print(document['_id'])
    #for stats in ip_user_agent_stats:
    #    print(f"{stats[0]}\t{stats[1]}\t{stats[2]}")   
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
