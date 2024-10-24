



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
ruta_excel = ruta_actual[0:-4] + "\\data_for_the_script.xlsx"

#Variables from excel

workbook = xl.Workbooks.Open(Filename = ruta_excel )
sheet = workbook.Sheets("main")
ini_date_pt = sheet.Cells(5, 3).Value
end_date_pt = sheet.Cells(5, 4).Value
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

if os.getcwd().split(os.sep)[-1] != name_dir:
        
        directory = os.getcwd()[0:-4] + f"\\{name_dir}"
print(directory)

#Downloading data we need

logger.info(f'Descargando informacion en el directorio : {name_dir}')
for name in names: 
    downloading_data.download_data(ini_date_pt, end_date_pt, name[0], name[1], directory)
cnxn = DB.setup()


#We call the methods the user said

if new_RR:
    logger.info("Creating new rows for table RR")
    DB.setRR(cnxn, ini_date_pt, end_date_pt, RR_interval)
if update_RR:
    logger.info("Updating rows for table RR")
    DB.updateRR(xl, cnxn, directory)
if new_aFRR_Energy:
    logger.info("Creating new rows for table aFRR_Energy")
    DB.set_aFRR_Energy(cnxn, ini_date_pt, end_date_pt, aFRR_Energy_interval)
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
    logger.info("Creating new rows for table aFRR_Power")
    DB.set_aFRR_power(cnxn, ini_date_pt, end_date_pt, aFRR_power_interval)
if update_aFRR_power: 
    logger.info("Updating rows for table aFRR_Power")
    DB.update_aFRR_power(xl, cnxn, directory)