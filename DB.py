

import pyodbc, sys, os
sys.path.append(os.getcwd() + '/lib/')
from logger import *
logger = MyLogger("Weekly ES | Main")
## Set logger level to 'logger_level' variable
logger_level = 20
addLogConsole(logger, logger_level)
server = 'SQLProdInt1.verit.dnv.com'
database = 'PPF_ES_AS'
import csv
def setup():
    
    logger.info("Connecting to db...")
    driver = 'ODBC Driver 17 for SQL Server' #driver from SQL Server Management Studio
    try: 
        # Define the connection string
        cnxn = pyodbc.connect(
           f'DRIVER={driver}; \
            SERVER={server}; \
            DATABASE={database}; \
            Trusted_Connection=yes;'
        )
        logger.info(f'Using {driver} driver for SQL connection')
    except: 
        driver = 'SQL Server' #driver from DBeaver
        try: 
            # Define the connection string
            cnxn = pyodbc.connect(
               f'DRIVER={driver}; \
                SERVER={server}; \
                DATABASE={database}; \
                Trusted_Connection=yes;'
            )
            logger.info(f'Using {driver} driver for SQL connection')
        except:
            logger.error(f'No valid SQL driver installed in your PC: please review documentation to install DBEaver of SQL Server Management Studio.')
            exit()

    # Create the cursor
    cursor = cnxn.cursor()
    cursor.fast_executemany = True


    
    return cursor 

def updateRR(xl, cnxn, dir_data):
    os.chdir(dir_data)
    archivos = os.listdir(dir_data)
    for k in range(0, len(archivos)):
        with open(archivos[k]) as csvfile:
            logger.info(f"CSV : {archivos[k]}...")
            reader= csv.reader(csvfile, delimiter=';')
            for row in reader:
                action = "INSERT INTO RR (DATE, HH, NN) VALUES (?, '0', '0')"
                cnxn.execute(action, row[0])
        cnxn.commit()
    