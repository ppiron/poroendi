from airflow.decorators import (
    dag,
    task,
)
from airflow.models import Variable
import pendulum
from pendulum import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import re

base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
# if not Variable.get("date"):
# Variable.set('date', '{"year": "2019", "month": "01" }')

@dag(
    schedule="0 2 2 1-12 *",
    start_date=datetime(2019, 2, 1),
    end_date=datetime(2019, 4, 1),
    catchup=True,
    default_args={
        "retries": 2
    },
    tags=["ny_taxi"]
)
def etl():
    @task()
    def get_year_and_month(**kwargs) -> str:
        # date = Variable.get("date", deserialize_json=True)
        exec_date = kwargs.get("ds")
        print(exec_date)
        return exec_date

    @task()
    def retrieve(exec_date) -> str:
        dt = pendulum.parse(exec_date)
        dt = dt.subtract(months=1)
        year = dt.format('YYYY')
        month = dt.format('MM')
        url = f"{base_url}green_tripdata_{year}-{month}.csv.gz"
        filename = f"green_tripdata_{year}-{month}.csv.gz" 
        response = requests.get(url)
        with open(filename, mode="wb")  as file:
            file.write(response.content)
        return filename
    
    @task()
    def stage(filename: str):
        df = pd.read_csv(filename).head(n=0)
        columns = list(df.columns)
        columns = [(lambda col: re.sub(r'(?<=[a-z])([A-Z])', r'_\1', col))(col) for col in columns]
        columns = [(lambda col: re.sub(r'(?<=[A-Z]{2})([A-Z])', r'_\1', col).lower())(col) for col in columns]
        # print(columns)
        print(df.dtypes)

            

    @task()
    def load(filename: str):
        engine = create_engine(f'postgresql+psycopg2://data_loader:data_loader_pass@localhost:5432/ny_taxi')
        df_iter = pd.read_csv(filename, chunksize=10000)
        for i, df in enumerate(df_iter):
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
            if i == 0:
                df.head(0).to_sql(name='green_tripdata', con=engine, if_exists='replace')
            df.to_sql(name='green_tripdata', con=engine, if_exists='append')

    exec_date = get_year_and_month()
    filename = retrieve(exec_date)
    load(filename)

etl()
