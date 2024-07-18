



from ctypes.wintypes import BOOL
import os, sys
import downloading_data
import makeDir
import subprocess
from win32com.client import Dispatch
sys.path.append(os.getcwd() + '/lib/')
from logger import *
logger = MyLogger("Weekly ES | Main")
## Set logger level to 'logger_level' variable
logger_level = 20
addLogConsole(logger, logger_level)
import pyodbc
import DB
import datetime
from datetime import timedelta
# from entsoe import EntsoePandasClient


# client = EntsoePandasClient(api_key='7f81335d-fb52-426e-9efd-34844e232606')

# Driver connection testing

def killTask (task: str): 
    try:
        subprocess.run(["taskkill", "/IM", task, "/F"], check=True)
    except:
        pass




killTask("EXCEL.EXE")
xl = Dispatch('Excel.Application')
xl.Visible = False
ruta_actual = os.getcwd()
ruta_excel = ruta_actual + "\\data_for_the_script.xlsx"
workbook = xl.Workbooks.Open(Filename = ruta_excel)
sheet = workbook.Sheets("week_week")
ini_date_pt = sheet.Cells(5, 3).Value
fecha_dt = datetime.datetime.strptime(ini_date_pt, '%Y-%m-%d')
nueva_fecha_dt = fecha_dt + timedelta(weeks=1)
end_date_pt = nueva_fecha_dt.strftime('%Y-%m-%d')
number_weeks = int(sheet.Cells(5, 4).Value)
new_RR = bool(sheet.Cells(10, 4).Value)
update_RR = bool(sheet.Cells(10, 5).Value)
RR_interval = sheet.Cells(10, 6).Value
new_aFRR_Energy = bool(sheet.Cells(11, 4).Value)
update_aFRR_Energy = bool(sheet.Cells(11, 5).Value)
aFRR_Energy_interval = sheet.Cells(11, 6).Value
new_mFRR_Energy = bool(sheet.Cells(12, 4).Value)
update_mFRR_Energy = bool(sheet.Cells(12, 5).Value)
mFRR_Energy_interval = sheet.Cells(12, 6).Value
new_aFRR_power = bool(sheet.Cells(13, 4).Value)
update_aFRR_power = bool(sheet.Cells(13, 5).Value)
aFRR_power_interval = sheet.Cells(13, 6).Value


workbook.Close()



for i in range(0, number_weeks):
    if i > 0:
        fecha_dt = datetime.datetime.strptime(ini_date_pt, '%Y-%m-%d')

        # Sumar una semana
        nueva_fecha_dt = fecha_dt + timedelta(weeks=1)

        # Convertir el objeto datetime de vuelta a una cadena
        ini_date_pt = nueva_fecha_dt.strftime('%Y-%m-%d')
        fecha_dt = datetime.datetime.strptime(end_date_pt, '%Y-%m-%d')

        # Sumar una semana
        nueva_fecha_dt = fecha_dt + timedelta(weeks=1)

        # Convertir el objeto datetime de vuelta a una cadena
        end_date_pt = nueva_fecha_dt.strftime('%Y-%m-%d')
    logger.info(f"First day of the week: {ini_date_pt}")
    logger.info(f"Last day of the week: {end_date_pt}")
    names = [
    
    ['Allocated aFRR Up', 632],
    ['Energy Price RR', 1782],
    ['Allocated aFRR Down', 633],
    ['Price allocated aFRR', 634],
    ['Weighted average price aFRR', 10388],
    ['Energy aFRR Up', 680],
    ['Energy aFRR Down', 681],
    ['Energy Price aFRR Up', 682],
    ['Energy Price aFRR Down', 683],
    ['Energy mFRR Up', 675],
    ['Energy mFRR Down', 674],
    ['Energy Price mFRR Up', 677],
    ['Energy Price mFRR Down', 676],
    ['Energy RR Up', 666],
    ['Energy RR Down', 667]


    ]



    name_dir = 'data'
    makeDir.makeDir(name_dir)
    directory = os.getcwd() + f"\\{name_dir}"
    print(directory)

    logger.info(f'Descargando informacion en el directorio : {name_dir}')
    for name in names: 
        downloading_data.download_data(ini_date_pt, end_date_pt, name[0], name[1], directory)
    cnxn = DB.setup()


    if new_RR:
        logger.info("Creating new rows for table RR")
        DB.setRR(cnxn, ini_date_pt, end_date_pt, RR_interval)
    if update_RR:
        logger.info("Updating rows for table RR")
        DB.updateRR(xl, cnxn, directory)
        

    if new_aFRR_Energy:
        DB.set_aFRR_Energy(cnxn, ini_date_pt, end_date_pt, aFRR_Energy_interval)
        logger.info("Creating new rows for table aFRR_Energy")
    if update_aFRR_Energy:
        logger.info("Updating rows for table aFRR_Energy")
        DB.update_aFRR_Energy(xl, cnxn, directory)
    if new_mFRR_Energy: 
        logger.info("Creating new rows for table mFRR_Energy")
        DB.set_mFRR_Energy(cnxn, ini_date_pt, end_date_pt, aFRR_Energy_interval)
    if update_mFRR_Energy:
        logger.info("Updating rows for table mFRR_Energy")
        DB.update_mFRR_Energy(xl, cnxn, directory)
    if new_aFRR_power:
        logger.info("Creating new rows for table aFRR_power")
        DB.set_aFRR_power(cnxn, ini_date_pt, end_date_pt, aFRR_power_interval)
    if update_aFRR_power: 
        logger.info("Updating rows for table aFRR_power")
        DB.update_aFRR_power(xl, cnxn, directory)