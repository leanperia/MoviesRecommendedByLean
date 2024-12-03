import duckdb
import pandas as pd
import os
import csv
import gc
import logging
from tqdm import tqdm

# I set up logging to debug kernel crashing
logging.basicConfig(
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('ingest_debug.log')
    ]
)

db = duckdb.connect('data.duckdb')

filenames = os.listdir('imdb')

CHUNK_SIZE = 100000

for filename in filenames:
    table_name = filename.replace('.', '_').replace('_tsv', '_raw')
    
    # Get the number of total lines for the progress bar
    total_lines = sum(1 for _ in open(f'imdb/{filename}'))
    logging.info(f"Total lines for {filename}: {total_lines}")
    total_chunks = total_lines // CHUNK_SIZE + 1

    # Create iterator for chunks
    chunks = pd.read_csv(f'imdb/{filename}', 
                         sep='\t',
                         quoting=csv.QUOTE_NONE, na_values='\\N',
                         chunksize=CHUNK_SIZE,
                         low_memory=False)

    # Create table from first chunk
    first_chunk = next(chunks)
    try:
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM first_chunk WHERE 1=0
        """)
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {str(e)}", exc_info=True)
        logging.debug(f"First chunk schema: {first_chunk.dtypes}")
        continue

    # Insert chunks and update the progress bar
    try:
        with tqdm(total=total_chunks) as pbar:
            # Insert first chunk
            db.execute(f"INSERT INTO {table_name} SELECT * FROM first_chunk")
            pbar.update(1)
            
            # Insert remaining chunks
            for chunk in chunks:
                try:
                    db.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
                except Exception as e:
                    logging.error(f"Error inserting chunk into {table_name}: {str(e)}", exc_info=True)
                    logging.debug(f"Problematic chunk info: {chunk.shape}, {chunk.dtypes}")
                    logging.debug(f"Sample of failing data:\n{chunk.head()}")
                pbar.update(1)
    except Exception as e:
        logging.error(f"Error processing file {filename}: {e}")
    finally:
        chunks.close()
        del first_chunk, chunks

db.close()