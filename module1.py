
import os
import json
import pandas as pd
import numpy

APIkey = '2b08d9cf036ea69012bfa2a06d6c13136b0ff91e9a9f148f09ac09216d237ec3'
timeZone = '+01'
ini_date_pt = "2024-07-06"
end_date_pt = "2024-07-12"

command = f'''curl "https://api.esios.ree.es/indicators/632?\
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
        -H "Cookie: "\
        > "prueba.json"''' 
os.system(command)
file = 'prueba.json'

with open(file, 'r', encoding="utf8") as f:
        data = json.loads(f.read())
            
        # Read as a dataframe
        df = pd.DataFrame(data["indicator"]["values"])
        df.to_csv(f'prueba.csv', index=False)
df = pd.DataFrame({'data':[]})
data = pd.read_csv(f'prueba.csv')
for index, row in data.iterrows():
            datetime = row.datetime
            isodate = datetime[0:10].replace('-','')
            hour = int(datetime[11:13]) + 1
            min = int(datetime[14:16]) 
            quantity = row.value
            row = f'{isodate};{hour};{min};{quantity}'
            df = df.append({'data': row}, ignore_index=True)
        
    # Export the main dataframe to CSV
df.to_csv(f'prueba_final.csv', header=False, index=False)