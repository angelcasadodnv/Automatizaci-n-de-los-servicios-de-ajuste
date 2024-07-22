


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
sheet = workbook.Sheets("search")
ini_date_pt = sheet.Cells(5, 3).Value
end_date_pt = sheet.Cells(5, 4).Value
tabla = sheet.Cells(5, 7).Value
interval = sheet.Cells(5, 6).Value


ini_datetime = datetime.datetime.strptime(ini_date_pt, '%Y-%m-%d')
final_datetime = datetime.datetime.strptime(end_date_pt, '%Y-%m-%d')
    
current_datetime = ini_datetime
delta = timedelta(minutes=interval)
    
days_interval = []
workbook.Close()  
while current_datetime <= final_datetime:
    date_str = current_datetime.strftime('%Y%m%d')
    add = ""
    add_2 = ""
    hour = current_datetime.hour
    if hour < 10 :
        add = "0"
    minute = current_datetime.minute
    if minute < 10 :
        add_2 = "0"

    days_interval.append(date_str + add + str(hour) + add_2 + str (minute))
    current_datetime += delta



logger.info(tabla)


name_dir = 'data'

directory = os.getcwd() + f"\\{name_dir}"

cnxn = DB.setup()

ini_date_pt = datetime.datetime.strptime(ini_date_pt, '%Y-%m-%d')
ini_date_pt = ini_date_pt.strftime('%Y%m%d')
action = f"SELECT id, Date, HH, NN FROM {tabla} where Date >= ? and Date <= ?;"
cnxn.execute(action, (ini_date_pt, end_date_pt))
result = cnxn.fetchall()
workbook = xl.Workbooks.Add()
sheet = workbook.Sheets(1)
workbook_2 = xl.Workbooks.Add()
sheet_2 = workbook_2.Sheets(1)

logger.info("Searching missing values and writting existing values...")
i = 1
for row in result:
    sheet.Cells(i, 1).Value = row[0]
    sheet.Cells(i, 2).Value = row[1]
    sheet.Cells(i, 3).Value = row[2]
    sheet.Cells(i, 4).Value = row[3]
    add_ = ""
    add_2 = ""

    if row[2] < 10:
        add_ = "0"
    if row[3] < 10:
        add_2 = "0"
    all_str = str(row[1]) + add_ + str(row[2]) + add_2 + str (row[3])
    if all_str in days_interval: 
        logger.info(all_str)
        days_interval.remove(all_str)
    i += 1

# Guardar el libro de Excel
file_path = os.getcwd() + '\\days_in_database.xlsx'
file_path_2 = os.getcwd() + '\\days_missing.xlsx'

i = 1
for data in days_interval:
    logger.info(data)
    sheet_2.Cells(i, 1).Value = data[0:8]
    sheet_2.Cells(i, 2).Value = data[8:10]
    sheet_2.Cells(i, 3).Value = data[10:12]
    sheet_2.Cells(i, 4).Value = data[12:14]
    i += 1
workbook.SaveAs(file_path)
workbook_2.SaveAs(file_path_2)


workbook.Close(SaveChanges=True)
workbook_2.Close(SaveChanges=True)
xl.Quit()

