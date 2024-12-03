import pandas as pd
import duckdb 
import os
import csv 
import gc
from tqdm import tqdm
from ydata_profiling import ProfileReport


db = duckdb.connect(database='imdb.duckdb') 

MAX_ROWS = 10_000_000 

filenames = os.listdir('imdb')
tablenames = [f.replace('.','_') for f in filenames]
tablenames = [f.replace('_tsv','_raw') for f in tablenames]

for tablename in tablenames:
    size = db.execute(f"SELECT count(*) FROM {tablename}").fetch_df().values[0][0]
    if size > MAX_ROWS:
        sampledf = db.execute(f"""SELECT * FROM {tablename} USING 
                                  SAMPLE {MAX_ROWS} ROWS;""").fetch_df()
        profile = ProfileReport(sampledf,  explorative=True) #minimal=True,
    else:
        sampledf = db.execute(f"SELECT * FROM {tablename};").fetch_df()
        profile = ProfileReport(sampledf, explorative=True)
    
    profile.to_file(f"profile_report_{tablename}.html")

db.close()