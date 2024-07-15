



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
ruta_excel = ruta_actual + "\\data_for_the_script.xlsx"
workbook = xl.Workbooks.Open(Filename = ruta_excel)
sheet = workbook.Sheets("main")
ini_date_pt = sheet.Cells(5, 3).Value
end_date_pt = sheet.Cells(5, 4).Value
workbook.Close()

names = [
    
['Allocated aFRR Up', 632],
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
['Energy mFRR Up', 666],
['Energy RR Down', 667],
['Energy Price RR', 1782]

]



name_dir = 'data'
makeDir.makeDir(name_dir)
directory = os.getcwd() + f"\\{name_dir}"
print(directory)

logger.info(f'Descargando informacion en el directorio : {name_dir}')
# for name in names: 
#     downloading_data.download_data(ini_date_pt, end_date_pt, name[0], name[1], directory)
cnxn = DB.setup()
DB.updateRR(xl, cnxn, directory)
os.chdir(directory)
print(os.listdir(directory))