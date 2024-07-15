#!./venv/bin/python
# -*- coding: utf-8 -*-

import datetime
import multiprocessing
import os
import sys
import threading
import subprocess
import time
import fx_mssql_query_insert

def main(argv):
    clock_start = datetime.datetime.now().astimezone() # Precision: microseconds (= 1/1000000 second).
    clock_end = clock_start
    elapsed_time = 0
    
    server_name_oslo = "SQLProdInt1.verit.dnv.com"
    
    # -----------------------------------------------------------------------

    print()
    print("------------------------------------------------------------")
    print("=> BEGIN at " + clock_start.isoformat()[0:19] + clock_start.isoformat()[26:])
    print()
    
    # print("1.- Tasks in serial, sequencial order...")
    # print()

    # 1.-
    # The list `tasks[]` stores all info about databases and CSV files involved.
    # Each task will have a specific script built with the aforementioned info,
    # and these scripts will be executed in a serial/sequential way, 
    # one by one in a loop.
    tasks = [
        {
        "database_name": "PPF_ES_PLT", 
        "table_name": "tblCO2", 
        "fields": "ISODate,CO2", 
        "checks": "1", 
        "csv_path": "csv/tblCO2.csv"
        },
        {
        "database_name": "PPF_ES_PLT", 
        "table_name": "tblSpots", 
        "fields": "ISODate,HH,Market,P", 
        "checks": "1,2,3", 
        "csv_path": "csv/tblSpotsMD.csv"
        },
        {
        "database_name": "PPF_ES_PLT", 
        "table_name": "tblSpots", 
        "fields": "ISODate,HH,Market,P", 
        "checks": "1,2,3", 
        "csv_path": "csv/tblSpotsMI.csv"
        },
        {
        "database_name": "PPF_ES_Bilaterals", 
        "table_name": "tblBilaterals", 
        "fields": "ISODate,HH,OMIEUnit,Q,ContractDetails,ContractType,ContractID", 
        "checks": "1", 
        "csv_path": "csv/tblBilaterales.csv"
        },
        {
        "database_name": "PPF_ES_I90", 
        "table_name": "tblI90", 
        "fields": "ISODate,Intervals,HH,NN,Market,Direction,REEUnit,Q,P", 
        "checks": "1", 
        "csv_path": "csv/tblI90.csv"
        },
        {
        "database_name": "PPF_ES_BidsDAM", 
        "table_name": "tblBidsDAM", 
        "fields": "ISODate,HH,Country,OMIEUnit,OrderType,Q,P,OrderStatus,Market", 
        "checks": "1,9", 
        "csv_path": "csv/tblPujasMD.csv"
        },
        {
        "database_name": "PPF_ES_BidsIM", 
        "table_name": "tblBidsIM", 
        "fields": "ISODate,HH,Country,OMIEUnit,OrderType,Q,P,OrderStatus,Market", 
        "checks": "1,9", 
        "csv_path": "csv/tblPujasMI.csv"
        },
        {
        "database_name": "PPF_ES_Renewables", 
        "table_name": "tblRenewables", 
        "fields": "ISODate,HH,ID,Q", 
        "checks": "1,3", 
        "csv_path": "csv/tblRenovables.csv"
        },
        {
        "database_name": "PPF_ES_MarginalTechnology", 
        "table_name": "tblMarginalTechnology", 
        "fields": "ISODate,HH,TechnologyID", 
        "checks": "1,2", 
        "csv_path": "csv/tblTecnologiaMarginal.csv"
        }
    ]
    n_tasks = len(tasks)

    # We have all info. Create the script per each task.
    for i in range(0, n_tasks): 
        script = f"./fx_mssql_query_insert.py \
            --server={server_name_oslo} \
            --database={tasks[i]['database_name']} \
            --table={tasks[i]['table_name']} \
            --fields={tasks[i]['fields']} \
            --separator=';' \
            --check={tasks[i]['checks']} \
            --n-rows-chunk 25000 \
            {tasks[i]['csv_path']}"
        script = " ".join(script.split()) # Remove consecutive spaces inside a string.
        tasks[i]["script"] = script # Add a new item "key:val" in the dictionaries.
        # print(f"=> script = {script}")

    # 2.-
    # Send scripts to execution, one by one in a loop.
    for i in range(0, n_tasks):
        print()
        print(f"[{i + 1}/{n_tasks}]")
        if (os.path.exists(tasks[i]["csv_path"]) == True):
            exit_status = subprocess.call(tasks[i]["script"], shell=True) # os.system(tasks[i]["script"])
            if (exit_status != 0):
                print()
                print(f"Error. Insertion failed on csv_path [= '{tasks[i]['csv_path']}'].")
                print()
        else:
            print()
            print(f"csv_path [= '{tasks[i]['csv_path']}'] doesn't exist.")
            print()


    # -----------------------------------------------------------------------

    # Completed.
    time.sleep(1)
    clock_end = datetime.datetime.now().astimezone()
    elapsed_time = clock_end - clock_start
    elapsed_time_str = str(elapsed_time)[0:str(elapsed_time).index(".")]
    print("=> END   at " + clock_end.isoformat()[0:19] + clock_end.isoformat()[26:])
    print("elapsed_time =", elapsed_time_str)
    print("exit = 0")
    print("------------------------------------------------------------")
    print()

    sys.exit(0)



if (__name__ == "__main__"):
    main(sys.argv[1:])
