import log_cruncher




def main():
    # processed_logs = log_cruncher.process_logs_csv("test_logs.csv")
    # processed_logs = log_cruncher.get_error_logs("test_logs.csv")
    processed_logs = log_cruncher.process_logs_json("test_logs.jsonl")
    
    for log in processed_logs:  
        print(log)
    
    
    print("first log level: ", processed_logs[0].level)


main()