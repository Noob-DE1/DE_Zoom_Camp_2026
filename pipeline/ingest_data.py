#pyright: reportMissingImports=false
import warnings
warnings.filterwarnings("ignore")

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest_data(url, dtype, parse_dates, chunksize,engine,target_table) -> pd.DataFrame:


    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator = True,
        chunksize = chunksize
    )

    first_chunk = next(df_iter)
    first_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')

    print(f"Table {target_table} created")

    first_chunk.to_sql(name=target_table, con=engine, if_exists='append')

    print(f"Inserted first chunk: {len(first_chunk)}")
    

    for df_chunk in tqdm(df_iter):

        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')
        print(f"Inserted chunk: {len(df_chunk)}")

    print(f"Data ingestion completed successfully to Postgres database table = {target_table}")

@click.command()
@click.option('--year', type=int, default=2021, help='Year of the data')
@click.option('--month', type=int, default=1, help='Month of the data')
@click.option('--pg-user', type=str, default='root', help='PostgreSQL username')
@click.option('--pg-password', type=str, default='root', help='PostgreSQL password')
@click.option('--pg-host', type=str, default='localhost', help='PostgreSQL host')
@click.option('--pg-port', type=int, default=5432, help='PostgreSQL port')
@click.option('--db-name', type=str, default='ny_taxi', help='Database name')
@click.option('--target-table', type=str, default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', type=int, default=100000, help='Chunk size for data ingestion')

def main(year, month, pg_user, pg_password, pg_host, pg_port, db_name, target_table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{db_name}')

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow" 

    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    
    ingest_data(url, dtype, parse_dates, chunksize, engine, target_table)

if __name__ == "__main__":
    main()
















