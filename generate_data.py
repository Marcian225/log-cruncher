import csv
import random
from datetime import datetime, timedelta

BUFFER_SIZE = 128 * 1024
levels = ['INFO', 'ERROR', 'WARN', 'DEBUG', 'FATAL']
messages = ["User logged in", "Database connection failed", "Retry attempt 1", "Cache miss", "Payment timeout"]


def python_generate():

    with open('massive_logs.csv', 'w', newline='', buffering=BUFFER_SIZE) as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'level', 'message', 'user_id'])
            

        start_time = datetime(2026, 3, 27, 10, 0, 0)
        for i in range(2_000_000):
            writer.writerow([
                (start_time + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                random.choice(levels),
                random.choice(messages),
                random.randint(0, 1000)
            ])
        print("Done generating massive_logs.csv")


import pandas as pd
def pandas_generate():

    start_time = datetime(2026, 3, 27, 10, 0, 0)
    df  = pd.DataFrame({
        'timestamp': [start_time + timedelta(seconds=i) for i in range(2_000_000)],
        'level': [random.choice(levels) for _ in range(2_000_000)],
        'message': [random.choice(messages) for _ in range(2_000_000)],
        'user_id': [random.randint(0, 1000) for _ in range(2_000_000)]
    })
    df.to_csv('massive_logs.csv', index=False, chunksize=BUFFER_SIZE)
    print("Done generating massive_logs.csv with pandas")


pandas_generate()