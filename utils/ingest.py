import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os
import argparse

def write_csv_to_postgres(file: str, table: str, engine: Engine):
    reader = pd.read_csv(file, chunksize=100000)
    for i, df in enumerate(reader):
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        if i == 0:
            df.head(0).to_sql(name=f'{table}', con=engine, if_exists='replace')
        df.to_sql(name=f'{table}', con=engine, if_exists='append')
        print(f"Inserted chunk number: {i}")

def main(args):
    engine = create_engine(f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}')
    url = args.url
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")
    
    write_csv_to_postgres(f"{csv_name}", f"{args.table_name}", engine)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--url', help='url of the csv file name')
    parser.add_argument('--table_name', help='name of the db table where we will write the results to')

    args = parser.parse_args()
    main(args)


# engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')