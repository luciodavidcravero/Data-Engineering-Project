'''
Este script obtiene datos de la API de los ultimos 180 días y los sube a la tabla carbon_intensity en Redshift
Esto se hace para llenar la tabla de datos y que no esté vacía inicialmente
'''

#Import required libraries
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests
from configparser import ConfigParser
from sqlalchemy import create_engine

#--------------------------------------------------------------------------
#Create table from last 6 months
#--------------------------------------------------------------------------
#API url and endpoint
headers = {
  'Accept': 'application/json'
}
base_url = 'https://api.carbonintensity.org.uk/intensity'
endpoint = 'stats'

#Get current date
current_date = datetime.utcnow()

#Calculate dates
from_date = current_date - timedelta(days=180)
from_date_str = from_date.strftime('%Y-%m-%dT00:00Z')
to_date_str = current_date.strftime('%Y-%m-%dT23:59Z')

block_date = 24

#Empty df to save each iteration data
dfs = []

#Loop to get data in 30 days intervals (API limit)
while from_date < current_date:
    to_date = from_date + timedelta(days=30)
    to_date_str = to_date.strftime('%Y-%m-%dT23:59Z')
    
    full_url = f"{base_url}/{endpoint}/{from_date_str}/{to_date_str}/{block_date}"
    
    r = requests.get(full_url,
                     params={},
                     headers=headers)
    
    if r.status_code == 200:
        print(f"Success for {from_date_str} to {to_date_str}")
        json_data = r.json()['data']
        df_carbon_intensity_180_days = pd.json_normalize(json_data)
        dfs.append(df_carbon_intensity_180_days)
    else:
        print(r.status_code)
        print(r.text)

    #Next interval
    from_date = to_date + timedelta(days=1)
    from_date_str = from_date.strftime('%Y-%m-%dT00:00Z')

#Concatenate all df in one
df_carbon_intensity_180_days = pd.concat(dfs, ignore_index=True)

#Save 6 months data to csv file
df_carbon_intensity_180_days.to_csv('../../data/raw/df_carbon_intensity_180_days.csv')

#--------------------------------------------------------------------------
#Connection to Redshift database
#--------------------------------------------------------------------------
#Read config file
config = ConfigParser()
config.read('config.cfg')

#Connection info
host = config.get('redshift', 'host')
database = config.get('redshift', 'database')
user = config.get('redshift', 'user')
password = config.get('redshift', 'password')
port = config.get('redshift', 'port')
dbengine = config.get('redshift', 'dbengine')

#Cadena de conexión para SQLAlchemy
conn_str = f"{dbengine}://{user}:{password}@{host}:{port}/{database}"

#Crea la conexión
engine = create_engine(conn_str)
conn = engine.connect()

#--------------------------------------------------------------------------
#Create empty tables on Redshift
#--------------------------------------------------------------------------
#Define SQL query
sql_query = '''
CREATE TABLE IF NOT EXISTS craverolucio_coderhouse.carbon_intensity (
    from_date VARCHAR(50) PRIMARY KEY,
    to_date VARCHAR(50) SORTKEY,
    intensity_max SMALLINT,
    intensity_average SMALLINT,
    intensity_min SMALLINT,
    intensity_index VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS craverolucio_coderhouse.stg_carbon_intensity (
    from_date VARCHAR(50) PRIMARY KEY,
    to_date VARCHAR(50) SORTKEY,
    intensity_max SMALLINT,
    intensity_average SMALLINT,
    intensity_min SMALLINT,
    intensity_index VARCHAR(50)
);
'''

# Ejecuta la sentencia SQL
conn.execute(sql_query)

#--------------------------------------------------------------------------
#Upload 180 days DataFrame to Redshift
#--------------------------------------------------------------------------
df_carbon_intensity_180_days = df_carbon_intensity_180_days.rename(columns=
                                                 {'from': 'from_date',
                                                  'to': 'to_date',
                                                  'intensity.max': 'intensity_max',
                                                  'intensity.average': 'intensity_average',
                                                  'intensity.min': 'intensity_min',
                                                  'intensity.index': 'intensity_index'})

#Upload to Redshift
df_carbon_intensity_180_days.to_sql(
    "carbon_intensity",
    conn,
    schema = "craverolucio_coderhouse",
    if_exists = "append", #La opción replace elimina la tabla y la crea a gusto de Pandas
    method = "multi", #Evita ejecutar 1 insert por cada registro
    chunksize = 100,
    index = False
)