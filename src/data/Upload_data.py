'''
Este script obtiene los datos del ultimo día y los sube a la tabla carbon_intensity (usando tabla stage)
'''

#Import required libraries
import pandas as pd
import requests
from datetime import datetime, timedelta
from configparser import ConfigParser
from sqlalchemy import create_engine

#--------------------------------------------------------------------------
#Get last-day data from API and save to df_carbon_intensity DataFrame
#--------------------------------------------------------------------------
headers = {
  'Accept': 'application/json'
}

base_url = 'https://api.carbonintensity.org.uk/intensity'
endpoint = 'stats'

#Get current date
current_date = datetime.utcnow()

#Calculate dates
from_date = current_date - timedelta(days=1)
from_date_str = from_date.strftime('%Y-%m-%dT00:00Z')
to_date_str = current_date.strftime('%Y-%m-%dT23:59Z')

block_date = 24 #Get data in block of 4 hours

full_url = f"{base_url}/{endpoint}/{from_date_str}/{to_date_str}/{block_date}"

r = requests.get(full_url,
                    params={},
                    headers=headers)

if r.status_code == 200:
    print(f"Success for {from_date_str} to {to_date_str}")
    json_data = r.json()['data']
    df_carbon_intensity = pd.json_normalize(json_data)
else:
    print(r.status_code)
    print(r.text)

#Rename columns on DataFrame
df_carbon_intensity = df_carbon_intensity.rename(columns=
                                                 {'from': 'from_date',
                                                  'to': 'to_date',
                                                  'intensity.max': 'intensity_max',
                                                  'intensity.average': 'intensity_average',
                                                  'intensity.min': 'intensity_min',
                                                  'intensity.index': 'intensity_index'})

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
#Create empty table on Redshift
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
#Upload last-day data to Redshift using stage table stg_carbon_intensity
#--------------------------------------------------------------------------
with engine.connect() as conn, conn.begin():
    conn.execute('TRUNCATE TABLE stg_carbon_intensity')
    df_carbon_intensity.to_sql(
        'stg_carbon_intensity',
        con = conn,
        index = False,
        method = 'multi',
        chunksize = 1000,
        if_exists = 'append'
    )

    conn.execute("""
    MERGE INTO carbon_intensity
    USING stg_carbon_intensity AS stg
    ON carbon_intensity.from_date = stg.from_date
    AND carbon_intensity.to_date = stg.to_date
    WHEN MATCHED THEN
        UPDATE SET
            intensity_max = stg.intensity_max,
            intensity_average = stg.intensity_average,
            intensity_min = stg.intensity_min,
            intensity_index = stg.intensity_index
    WHEN NOT MATCHED THEN
        INSERT (from_date, to_date, intensity_max, intensity_average, intensity_min, intensity_index)
        VALUES (stg.from_date, stg.to_date, stg.intensity_max, stg.intensity_average, stg.intensity_min, stg.intensity_index);
    """)