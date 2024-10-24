

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
import datetime
from datetime import timedelta
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

def setRR(cnxn, ini_date, final_date, RR_interval):
    ini_datetime = datetime.datetime.strptime(ini_date, '%Y-%m-%d')
    final_datetime = datetime.datetime.strptime(final_date, '%Y-%m-%d')

    current_datetime = ini_datetime
    delta = timedelta(minutes=RR_interval)
    
    insert_rows = []  

    while current_datetime <= final_datetime:
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute

     
        action = "SELECT id FROM RR WHERE Date = ? AND HH = ? AND NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()

       
        if not result:
            insert_rows.append((date_str, hour, minute))

        current_datetime += delta


    if insert_rows:
        insert_action = "INSERT INTO RR (DATE, HH, NN) VALUES (?, ?, ?)"
        cnxn.executemany(insert_action, insert_rows)
        logger.info(f"{len(insert_rows)} row(s) inserted.")

    cnxn.commit()
    logger.info("Empty rows created if needed.")

def set_aFRR_Energy(cnxn, ini_date, final_date, aFRR_Energy_interval):
    ini_datetime = datetime.datetime.strptime(ini_date, '%Y-%m-%d')
    final_datetime = datetime.datetime.strptime(final_date, '%Y-%m-%d')

    current_datetime = ini_datetime
    delta = timedelta(minutes=aFRR_Energy_interval)
    
    insert_rows = [] 

    while current_datetime <= final_datetime:
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute

     
        action = "SELECT id FROM aFRR_Energy WHERE Date = ? AND HH = ? AND NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()

     
        if not result:
            insert_rows.append((date_str, hour, minute))

        current_datetime += delta

  
    if insert_rows:
        insert_action = "INSERT INTO aFRR_Energy (DATE, HH, NN) VALUES (?, ?, ?)"
        cnxn.executemany(insert_action, insert_rows)
        logger.info(f"{len(insert_rows)} row(s) inserted into aFRR_Energy.")

    cnxn.commit()
    logger.info("Empty rows created in aFRR_Energy where necessary.")
    return

def set_mFRR_Energy(cnxn, ini_date, final_date, aFRR_Energy_interval):
    ini_datetime = datetime.datetime.strptime(ini_date, '%Y-%m-%d')
    final_datetime = datetime.datetime.strptime(final_date, '%Y-%m-%d')

    current_datetime = ini_datetime
    delta = timedelta(minutes=aFRR_Energy_interval)
    
    insert_rows = [] 

    while current_datetime <= final_datetime:
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute

        
        action = "SELECT id FROM mFRR_Energy WHERE Date = ? AND HH = ? AND NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()

       
        if not result:
            insert_rows.append((date_str, hour, minute))

        current_datetime += delta

    
    if insert_rows:
        insert_action = "INSERT INTO mFRR_Energy (DATE, HH, NN) VALUES (?, ?, ?)"
        cnxn.executemany(insert_action, insert_rows)
        logger.info(f"{len(insert_rows)} row(s) inserted into mFRR_Energy.")

    cnxn.commit()  
    logger.info("Empty rows created in mFRR_Energy where necessary.")
    return
def set_aFRR_power(cnxn, ini_date, final_date, aFRR_Energy_interval):
    ini_datetime = datetime.datetime.strptime(ini_date, '%Y-%m-%d')
    final_datetime = datetime.datetime.strptime(final_date, '%Y-%m-%d')

    current_datetime = ini_datetime
    delta = timedelta(minutes=aFRR_Energy_interval)
    
    insert_rows = []  

    while current_datetime <= final_datetime:
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute

       
        action = "SELECT id FROM aFRR_power WHERE Date = ? AND HH = ? AND NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()

        
        if not result:
            insert_rows.append((date_str, hour, minute))

        current_datetime += delta

    
    if insert_rows:
        insert_action = "INSERT INTO aFRR_power (DATE, HH, NN) VALUES (?, ?, ?)"
        cnxn.executemany(insert_action, insert_rows)
        logger.info(f"{len(insert_rows)} row(s) inserted into aFRR_power.")

    cnxn.commit()
    logger.info("Empty rows created in aFRR_power where necessary.")
    return

def update_mFRR_Energy(xl, cnxn, dir_data):
    csvRR = ['Energy mFRR Up.csv', 'Energy mFRR Down.csv', 'Energy Price mFRR Up.csv', 'Energy Price mFRR Down.csv']
    os.chdir(dir_data)

    update_queries = [
        "UPDATE mFRR_Energy SET Energy_mFRR_Up = ? WHERE id = ?",
        "UPDATE mFRR_Energy SET Energy_mFRR_Down = ? WHERE id = ?",
        "UPDATE mFRR_Energy SET Energy_Price_mFRR_Up = ? WHERE id = ?",
        "UPDATE mFRR_Energy SET Energy_Price_mFRR_Down = ? WHERE id = ?"
    ]

    for k, csv_file in enumerate(csvRR):
        with open(csv_file) as csvfile:
            logger.info(f"CSV: {csv_file}...")
            reader = csv.reader(csvfile, delimiter=';')

            updates = []  

            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                select_action = "SELECT id FROM mFRR_Energy WHERE Date = ? AND HH = ? AND NN = ?"
                cnxn.execute(select_action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()

                for r in result:
                    id_row = r[0]

                  
                    updates.append((row[3], id_row))

                    logger.info(f"Actualizado el valor en el CSV {csv_file} con fecha {row[0]}, HH {row[1]}, NN {row[2]} y valor {row[3]}.")

            
            cnxn.executemany(update_queries[k], updates)
            cnxn.commit()




def update_aFRR_Energy(xl, cnxn, dir_data):
    csvRR = ['Energy aFRR Up.csv', 'Energy aFRR Down.csv', 'Energy Price aFRR Up.csv', 'Energy Price aFRR Down.csv']
    os.chdir(dir_data)

    update_queries = [
        "UPDATE aFRR_Energy SET Energy_aFRR_Up = ? WHERE id = ?",
        "UPDATE aFRR_Energy SET Energy_aFRR_Down = ? WHERE id = ?",
        "UPDATE aFRR_Energy SET Energy_Price_aFRR_Up = ? WHERE id = ?",
        "UPDATE aFRR_Energy SET Energy_Price_aFRR_Down = ? WHERE id = ?"
    ]

    for k, csv_file in enumerate(csvRR):
        with open(csv_file) as csvfile:
            logger.info(f"CSV: {csv_file}...")
            reader = csv.reader(csvfile, delimiter=';')

            updates = []  

            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                select_action = "SELECT id FROM aFRR_Energy WHERE Date = ? AND HH = ? AND NN = ?"
                cnxn.execute(select_action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()

                if result:  
                    id_row = result[0][0] 

                   
                    updates.append((row[3], id_row))

                    logger.info(f"Preparando actualizar el valor en el CSV {csv_file} con fecha {row[0]}, HH {row[1]}, NN {row[2]} y valor {row[3]}.")

            
            if updates:
                cnxn.executemany(update_queries[k], updates)
                cnxn.commit()  


def updateRR(xl, cnxn, dir_data):
    csvRR = ['Energy RR Up.csv', 'Energy RR Down.csv', 'Energy Price RR.csv']
    os.chdir(dir_data)
    
    
    if not os.path.exists('Energy Price RR.csv'):
        csvRR.remove('Energy Price RR.csv')

    update_queries = [
        "UPDATE RR SET Energy_RR_UP = ? WHERE id = ?",
        "UPDATE RR SET Energy_RR_DOWN = ? WHERE id = ?",
        "UPDATE RR SET Energy_Price_RR = ? WHERE id = ?"
    ]

    for k, csv_file in enumerate(csvRR):
        with open(csv_file) as csvfile:
            logger.info(f"CSV: {csv_file}...")
            reader = csv.reader(csvfile, delimiter=';')
            updates = []  

            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                select_action = "SELECT id FROM RR WHERE Date = ? AND HH = ? AND NN = ?"
                cnxn.execute(select_action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()

                if result: 
                    id_row = result[0][0] 

                   
                    updates.append((row[3], id_row))
                    logger.info(f"Preparando actualizar el valor en el CSV {csv_file} con fecha {row[0]}, HH {row[1]}, NN {row[2]} y valor {row[3]}.")

          
            if updates:
                cnxn.executemany(update_queries[k], updates)
                cnxn.commit() 
      
def update_aFRR_power(xl, cnxn, dir_data):
    csvRR = ['Allocated aFRR Up.csv', 'Allocated aFRR Down.csv', 'Price allocated aFRR.csv', 'Weighted average price aFRR.csv']
    os.chdir(dir_data)


    update_queries = [
        "UPDATE aFRR_power SET Allocated_aFRR_Up = ? WHERE id = ?",
        "UPDATE aFRR_power SET Allocated_aFRR_Down = ? WHERE id = ?",
        "UPDATE aFRR_power SET Price_allocated_aFRR = ? WHERE id = ?",
        "UPDATE aFRR_power SET Weighted_average_price_aFRR = ? WHERE id = ?"
    ]

    for k, csv_file in enumerate(csvRR):
        with open(csv_file) as csvfile:
            logger.info(f"CSV: {csv_file}...")
            reader = csv.reader(csvfile, delimiter=';')
            updates = [] 

            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                select_action = "SELECT id FROM aFRR_power WHERE Date = ? AND HH = ? AND NN = ?"
                cnxn.execute(select_action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()

                if result:  
                    id_row = result[0][0] 
                    #Api data come with too many decimal numbers for Weighted_average_price_aFRR
                    try:
                        row[3] = round(float(row[3]), 5)
                    except:
                        pass
                    updates.append((row[3], id_row))
                    logger.info(f"Preparando actualizar el valor en el CSV {csv_file} con fecha {row[0]}, HH {row[1]}, NN {row[2]} y valor {row[3]}.")

            if updates:
                cnxn.executemany(update_queries[k], updates)
                cnxn.commit()  