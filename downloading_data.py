
import os
import json
import pandas as pd
import numpy

APIkey = '2b08d9cf036ea69012bfa2a06d6c13136b0ff91e9a9f148f09ac09216d237ec3'
timeZone = '+01'
import sys
sys.path.append(os.getcwd() + '/lib/')
from logger import *
logger = MyLogger("Weekly ES | Main")
## Set logger level to 'logger_level' variable
logger_level = 20
addLogConsole(logger, logger_level)
def download_data(ini_date_pt, end_date_pt, name, indicator, dir):
    os.chdir(dir)
    command = f'''curl "https://api.esios.ree.es/indicators/{indicator}?\
datetime=&\
start_date={ini_date_pt}T00:00:00{timeZone}:00&\
end_date={end_date_pt}T23:55:00{timeZone}:00&\
time_agg=&\
time_trunc=hour&\
geo_agg=sum&\
geo_ids=&\
geo_trunc=&\
locale=en" \
    -X GET \
    -H "Accept: application/json; application/vnd.esios-api-v1+json" \
    -H "Content-Type: application/json" \
    -H "Host: api.esios.ree.es" \
    -H "x-api-key:\"{APIkey}\"" \
    -H "Cookie: " \
    > "{name}.json"''' 
    os.system(command)
    file = f'{name}.json'


    with open(file, 'r', encoding="utf8") as f:
            data = json.loads(f.read())
            
            # Read as a dataframe
            df = pd.DataFrame(data["indicator"]["values"])
            df.to_csv(f'{name}.json', index=False)
    df = pd.DataFrame({'data':[]})
    data = pd.read_csv(f'{name}.json')
    for index, row in data.iterrows():
                if indicator ==  1782 and row.geo_name == 'Spain':
                    datetime = row.datetime
                    isodate = datetime[0:10].replace('-','')
                    hour = int(datetime[11:13]) 
                    datetime = row.tz_time
                    min = int(datetime[14:16]) 
                    quantity = row.value
                    row = f'{isodate};{hour};{min};{quantity};{row.geo_name}'
                    df = df.append({'data': row}, ignore_index=True)
                elif indicator != 1782:
                    datetime = row.datetime
                    isodate = datetime[0:10].replace('-','')
                    hour = int(datetime[11:13]) 
                    datetime = row.tz_time
                    min = int(datetime[14:16]) 
                    quantity = row.value
                    row = f'{isodate};{hour};{min};{quantity}'
                    df = df.append({'data': row}, ignore_index=True)
        
        # Export the main dataframe to CSV
    os.remove(f'{name}.json')        
    df.to_csv(f'{name}.csv', header=False, index=False)