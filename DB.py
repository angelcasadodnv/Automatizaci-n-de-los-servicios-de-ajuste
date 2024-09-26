

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
    
    result = []
    
    while current_datetime <= final_datetime:
        #result.append(f"{current_datetime.strftime('%Y-%m-%d')}, {current_datetime.hour}, {current_datetime.minute}")
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute
        action = "SELECT id FROM RR where Date = ? and HH = ? and NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()
        id_row = None
        for r in result:
             id_row = (r)
        if id_row == None:
            action = "INSERT INTO RR (DATE, HH, NN) VALUES (?, ?, ?)"
            logger.info(f'{date_str}, {hour}, {minute}')
            cnxn.execute(action, (date_str, hour, minute))
        current_datetime += delta
    logger.info("Empty row's created")
    cnxn.commit()
    return 
def set_aFRR_Energy(cnxn, ini_date, final_date, aFRR_Energy_interval):
    ini_datetime = datetime.datetime.strptime(ini_date, '%Y-%m-%d')
    final_datetime = datetime.datetime.strptime(final_date, '%Y-%m-%d')
    
    current_datetime = ini_datetime
    delta = timedelta(minutes=aFRR_Energy_interval)
    
    result = []
    
    while current_datetime <= final_datetime:
        #result.append(f"{current_datetime.strftime('%Y-%m-%d')}, {current_datetime.hour}, {current_datetime.minute}")
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute
        action = "SELECT id FROM aFRR_Energy where Date = ? and HH = ? and NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()
        id_row = None
        for r in result:
             id_row = (r)
        if id_row == None:
            action = "INSERT INTO aFRR_Energy (DATE, HH, NN) VALUES (?, ?, ?)"
            logger.info(f'{date_str}, {hour}, {minute}')
            cnxn.execute(action, (date_str, hour, minute))
        current_datetime += delta
    logger.info("Empty row's created")
    cnxn.commit()
    return 
def set_mFRR_Energy(cnxn, ini_date, final_date, aFRR_Energy_interval):
    ini_datetime = datetime.datetime.strptime(ini_date, '%Y-%m-%d')
    final_datetime = datetime.datetime.strptime(final_date, '%Y-%m-%d')
    
    current_datetime = ini_datetime
    delta = timedelta(minutes=aFRR_Energy_interval)
    
    result = []
    
    while current_datetime <= final_datetime:
        #result.append(f"{current_datetime.strftime('%Y-%m-%d')}, {current_datetime.hour}, {current_datetime.minute}")
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute
        action = "SELECT id FROM mFRR_Energy where Date = ? and HH = ? and NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()
        id_row = None
        for r in result:
             id_row = (r)
        if id_row == None:
            action = "INSERT INTO mFRR_Energy (DATE, HH, NN) VALUES (?, ?, ?)"
            logger.info(f'{date_str}, {hour}, {minute}')
            cnxn.execute(action, (date_str, hour, minute))
        current_datetime += delta
    logger.info("Empty row's created")
    cnxn.commit()
    return
def set_aFRR_power(cnxn, ini_date, final_date, aFRR_Energy_interval):
    ini_datetime = datetime.datetime.strptime(ini_date, '%Y-%m-%d')
    final_datetime = datetime.datetime.strptime(final_date, '%Y-%m-%d')
    
    current_datetime = ini_datetime
    delta = timedelta(minutes=aFRR_Energy_interval)
    
    result = []
    
    while current_datetime <= final_datetime:
        #result.append(f"{current_datetime.strftime('%Y-%m-%d')}, {current_datetime.hour}, {current_datetime.minute}")
        date_str = current_datetime.strftime('%Y%m%d')
        hour = current_datetime.hour
        minute = current_datetime.minute
        action = "SELECT id FROM aFRR_power where Date = ? and HH = ? and NN = ?;"
        cnxn.execute(action, (date_str, hour, minute))
        result = cnxn.fetchall()
        id_row = None
        for r in result:
             id_row = (r)
        if id_row == None:
            action = "INSERT INTO aFRR_power (DATE, HH, NN) VALUES (?, ?, ?)"
            logger.info(f'{date_str}, {hour}, {minute}')
            cnxn.execute(action, (date_str, hour, minute))
        current_datetime += delta
    logger.info("Empty row's created")
    cnxn.commit()
    return

def update_mFRR_Energy(xl, cnxn, dir_data):
    csvRR = ['Energy mFRR Up.csv', 'Energy mFRR Down.csv', 'Energy Price mFRR Up.csv', 'Energy Price mFRR Down.csv']
    os.chdir(dir_data)
    for k in range(0, len(csvRR)):
        with open(csvRR[k]) as csvfile:
            logger.info(f"CSV : {csvRR[k]}...")
            reader= csv.reader(csvfile, delimiter=';')
            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")

                # Convierte el objeto datetime al nuevo formato
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                action = "SELECT id FROM mFRR_Energy where Date = ? and HH = ? and NN = ?;"
                cnxn.execute(action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()
                for r in result:
                    id_row = (r)
                    
                if k == 0: 
                    action = "UPDATE mFRR_Energy SET Energy_mFRR_Up = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_mFRR_Up al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
                elif k== 1:
                    action = "UPDATE mFRR_Energy SET Energy_mFRR_DOWN = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_mFRR_DOWN al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")

                elif k == 2:
                    action = "UPDATE mFRR_Energy SET Energy_Price_mFRR_Up = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_Price_mFRR_Up al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
                elif k == 3: 
                    action = "UPDATE mFRR_Energy SET Energy_Price_mFRR_Down = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_Price_mFRR_Down al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
        cnxn.commit() 




def update_aFRR_Energy(xl, cnxn, dir_data):
    csvRR = ['Energy aFRR Up.csv', 'Energy aFRR Down.csv', 'Energy Price aFRR Up.csv', 'Energy Price aFRR Down.csv']
    os.chdir(dir_data)
    for k in range(0, len(csvRR)):
        with open(csvRR[k]) as csvfile:
            logger.info(f"CSV : {csvRR[k]}...")
            reader= csv.reader(csvfile, delimiter=';')
            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")

                # Convierte el objeto datetime al nuevo formato
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                action = "SELECT id FROM aFRR_Energy where Date = ? and HH = ? and NN = ?;"
                cnxn.execute(action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()
                for r in result:
                    id_row = (r)
                
                if k == 0: 
                    action = "UPDATE aFRR_Energy SET Energy_aFRR_Up = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_aFRR_Up al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
                elif k== 1:
                    action = "UPDATE aFRR_Energy SET Energy_aFRR_DOWN = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_aFRR_DOWN al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")

                elif k == 2:
                    action = "UPDATE aFRR_Energy SET Energy_Price_aFRR_Up = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_Price_aFRR_Up al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
                elif k == 3: 
                    action = "UPDATE aFRR_Energy SET Energy_Price_aFRR_Down = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_Price_aFRR_Down al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
        cnxn.commit()   


def updateRR(xl, cnxn, dir_data):
    csvRR = ['Energy RR Up.csv', 'Energy RR Down.csv', 'Energy Price RR.csv']
    os.chdir(dir_data)
    if not os.path.exists('Energy Price RR.csv'):
        csvRR.remove('Energy Price RR.csv')
    for k in range(0, len(csvRR)):
        with open(csvRR[k]) as csvfile:
            logger.info(f"CSV : {csvRR[k]}...")
            reader= csv.reader(csvfile, delimiter=';')
            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")

                # Convierte el objeto datetime al nuevo formato
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                action = "SELECT id FROM RR where Date = ? and HH = ? and NN = ?;"
                cnxn.execute(action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()
                for r in result:
                    id_row = (r)
                
                if k == 0: 
                    action = "UPDATE RR SET Energy_RR_UP = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_RR_UP al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
                elif k== 1:
                    action = "UPDATE RR SET Energy_RR_DOWN = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_RR_Down al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")

                elif k == 2:
                    action = "UPDATE RR SET Energy_Price_RR = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Energy_Price_RR al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
        cnxn.commit()
      
def update_aFRR_power(xl, cnxn, dir_data):
    csvRR = ['Allocated aFRR Up.csv', 'Allocated aFRR Down.csv', 'Price allocated aFRR.csv', 'Weighted average price aFRR.csv']
    os.chdir(dir_data)
    for k in range(0, len(csvRR)):
        with open(csvRR[k]) as csvfile:
            logger.info(f"CSV : {csvRR[k]}...")
            reader= csv.reader(csvfile, delimiter=';')
            for row in reader:
                fecha_obj = datetime.datetime.strptime(row[0], "%Y%m%d")

                # Convierte el objeto datetime al nuevo formato
                fecha_formateada = fecha_obj.strftime("%Y%m%d")
                action = "SELECT id FROM aFRR_power where Date = ? and HH = ? and NN = ?;"
                cnxn.execute(action, (fecha_formateada, row[1], row[2]))
                result = cnxn.fetchall()
                for r in result:
                    id_row = (r)
                
                if k == 0: 
                    action = "UPDATE aFRR_power SET Allocated_aFRR_Up = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Allocated_aFRR_Up al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
                elif k== 1:
                    action = "UPDATE aFRR_power SET Allocated_aFRR_Down = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Allocated_aFRR_Down al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")

                elif k == 2:
                    action = "UPDATE aFRR_power SET Price_allocated_aFRR = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Price_allocated_aFRR al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
    
                elif k == 3:
                    action = "UPDATE aFRR_power SET Weighted_average_price_aFRR = ? WHERE id = ?"
                    cnxn.execute(action, (row[3], id_row[0]))
                    logger.info(f"Actualiazado el valor Weighted_average_price_aFRR al valor {row[3]}, con fecha {row[0]}, {row[1]}, {row[2]}")
        cnxn.commit()