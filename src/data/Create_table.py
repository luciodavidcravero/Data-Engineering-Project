#Import required libraries
import pandas as pd
from configparser import ConfigParser
from sqlalchemy import create_engine

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
#Create table on Redshift
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
'''

# Ejecuta la sentencia SQL
conn.execute(sql_query)

#--------------------------------------------------------------------------
#Upload DataFrame to Redshift
#--------------------------------------------------------------------------
#Read csv file
df_carbon_intensity = pd.read_csv('../../data/raw/df_carbon_intensity.csv')
df_carbon_intensity = df_carbon_intensity.drop(columns='Unnamed: 0')
df_carbon_intensity = df_carbon_intensity.rename(columns=
                                                 {'from': 'from_date',
                                                  'to': 'to_date',
                                                  'intensity.max': 'intensity_max',
                                                  'intensity.average': 'intensity_average',
                                                  'intensity.min': 'intensity_min',
                                                  'intensity.index': 'intensity_index'})

#Upload to Redshift
df_carbon_intensity.to_sql(
    "carbon_intensity",
    conn,
    schema = "craverolucio_coderhouse",
    if_exists = "append", #La opción replace elimina la tabla y la crea a gusto de Pandas
    method = "multi", #Evita ejecutar 1 insert por cada registro
    chunksize = 100,
    index = False
)