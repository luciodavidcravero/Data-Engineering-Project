'''
En este archivo se definen las funciones para ser utilizadas en el DAG "dag_carbon_intensity"
'''

#Import required libraries
import pandas as pd
from utils import *

def load_carbon_intensity_data(config_file, start, end):
    """Proceso ETL para los datos de Carbon Intensity de la API

    Args:
        config_file (.cfg): file with database credentials
        start: fecha inicial
        end: fecha final
    """
    base_url = 'https://api.carbonintensity.org.uk/intensity'
    endpoint = 'stats'
    try:
        df_carbon_intensity = get_data(base_url,
                                       endpoint,
                                       start,
                                       end,
                                       24)
        
        engine = connect_to_db(config_file, "redshift")
        load_to_sql(df_carbon_intensity, "carbon_intensity", engine)
    except Exception as e:
        logging.error(f"Error al obtener datos de {base_url}: {e}")
        raise e