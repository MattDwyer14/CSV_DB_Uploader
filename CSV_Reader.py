import pandas as pd
import chardet
from sqlalchemy import create_engine, exc
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
# Capture warnings with logging
logging.captureWarnings(True)

# Credentials and file path
file_path = r""

username = ''
password = ''  
host = ''
port = ''
database = ''

table_name = ''
database_update_type = ''

def main():
    try:
        start_time = time.time()
        tot_start_time = time.time()
        logging.info(" (1/5) Opening file, detecting encoding...")
        with open(file_path, 'rb') as file:
            result = chardet.detect(file.read())
            encoding = result['encoding']
            logging.info(f" file encoding is: {encoding}")
    except FileNotFoundError as e:
        logging.error(f" Error: {e}")
        return

    try:
        end_time = time.time()
        elapsed_time = round(end_time-start_time, 2)
        logging.info(f"  Complete - [{elapsed_time}s]")
        start_time = time.time()
        logging.info(" (2/5) Reading CSV...")
        df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
    except pd.errors.ParserError as e:
        logging.error(f" Could not read file: {e}")
        return

    try:
        end_time = time.time()
        elapsed_time = round(end_time-start_time, 2)
        logging.info(f"  Complete - [{elapsed_time}s]")
        start_time = time.time()
        logging.info(" (3/5) Converting all columns to string and preserving null values...")
        # Convert all columns to strings and replace 'nan' and 'NaT' with None
        df = df.applymap(lambda x: None if pd.isna(x) else str(x))
    except Exception as e:
        logging.error(f" Could not convert to string: {e}")
        return
    except FutureWarning as e:
        logging.warning(f" FutureWarning encountered: {e}")

    try:
        end_time = time.time()
        elapsed_time = round(end_time-start_time, 2)
        logging.info(f"  Complete - [{elapsed_time}s]")
        start_time = time.time()
        logging.info(" (4/5) Creating database connection...")
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
    except exc.SQLAlchemyError as e:
        logging.error(f" Could not connect to database: {e}")
        return

    try:
        end_time = time.time()
        elapsed_time = round(end_time-start_time, 2)
        logging.info(f"  Complete - [{elapsed_time}s]")
        start_time = time.time()
        logging.info(" (5/5) Writing to Database...")
        df.to_sql(table_name, engine, if_exists=database_update_type, index=False)
        success = True
    except exc.SQLAlchemyError as e:
        logging.error(f" Could not write to database: {e}")

    finally:
        engine.dispose()

    if success == True:
        end_time = time.time()
        elapsed_time = round(end_time-start_time, 2)
        logging.info(f"  Complete - [{elapsed_time}s]")
        start_time = time.time()
        runtime = round(end_time-tot_start_time, 2)
        logging.info(f" Upload successful to {database}-{table_name} with {encoding} encoding."
                     f"Total elapsed time: [{runtime}s]")
    else:
        end_time = time.time()
        elapsed_time = round(end_time-start_time, 2)
        logging.info(f"  Incomplete - [{elapsed_time}s]")
        start_time = time.time()
        runtime = round(end_time-tot_start_time, 2)
        logging.info(f" Upload unsuccessful to {database}-{table_name} with {encoding} encoding."
                     f" Total elapsed time: [{runtime}s]")

if __name__ == "__main__":
    main()
