import duckdb
import pandas as pd
import os
import csv
import gc
import logging
from tqdm import tqdm
from ydata_profiling import ProfileReport

# I included data profiling as well for these datasets

# I set up logging to debug kernel crashing
logging.basicConfig(
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('ingest_debug.log')
    ]
)

db = duckdb.connect('imdb.duckdb')

def ingest_and_profile(directory, prefix):

    file_names = os.listdir(directory)

    for file_name in file_names:
        table_name = file_name.replace('.csv', '')
        table_name = prefix+ table_name
        print(f'processing {file_name} with table name {table_name}')
        
        total_lines = sum(1 for _ in open(f'{directory}/{file_name}'))
        logging.info(f"Total lines for {file_name}: {total_lines}")

        df = pd.read_csv(f'{directory}/{file_name}',low_memory=False)

        try:
            db.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * FROM df
            """)
            profile = ProfileReport(df, explorative=True)
            profile.to_file(f"profiling/profile_report_{table_name}.html")
        except Exception as e:
            logging.error(f"Error with table {table_name}: {str(e)}", exc_info=True)
            logging.debug(f"First chunk schema: {df.dtypes}")
            continue

ingest_and_profile('movielens', 'ml_')
ingest_and_profile('movielens32m', 'ml32_')


tmdb = pd.read_csv('TMDB_movie_dataset_v11.csv')
db.execute(f"""
    CREATE TABLE IF NOT EXISTS tmdb AS 
    SELECT * FROM tmdb
""")

profile = ProfileReport(tmdb, explorative=True)
profile.to_file(f"profiling/profile_report_tmdb.html")

db.close()