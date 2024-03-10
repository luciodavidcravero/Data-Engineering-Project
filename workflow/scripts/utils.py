import logging
import json
from configparser import ConfigParser
from datetime import datetime
import requests
from sqlalchemy import create_engine
from pandas import json_normalize

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data(base_url, endpoint, start, end, block):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parametros:
    base_url: La URL base de la API.
    endpoint: El endpoint (ruta) de la API para obtener datos específicos.
    start: start time
    end: end time
    block: Get block average Carbon Intensity statistics (average, max, mean) between from and to datetime
           The maximum date range is limited to 30 days
           The block length must be between 1 and 24 hours and should be an integer
           All times provided in UTC (+00:00)
    
    Retorno:
    Un DataFrame con los datos obtenidos de la API.
    """
    headers = {
        'Accept': 'application/json'
    }
    try:
        start_dt = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S%z')
        end_dt = datetime.strptime(end, '%Y-%m-%dT%H:%M:%S%z')
        start_date_str = start_dt.strftime('%Y-%m-%dT00:00Z')
        end_date_str = end_dt.strftime('%Y-%m-%dT23:59Z')
        endpoint_url = f"{base_url}/{endpoint}/{start_date_str}/{end_date_str}/{block}"
        logging.info(f"Obteniendo datos de {endpoint_url}...")
        logging.info(f"Desde: {start_date_str}")
        logging.info(f"Hasta: {end_date_str}")
        response = requests.get(endpoint_url,
                                params={},
                                headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.
        logging.info(response.url)
        logging.info("Datos obtenidos exitosamente... Procesando datos...")
        data = response.json()
        data = data["data"]
        df = json_normalize(data)
        df = df.rename(columns=
                        {'from': 'from_date',
                        'to': 'to_date',
                        'intensity.max': 'intensity_max',
                        'intensity.average': 'intensity_average',
                        'intensity.min': 'intensity_min',
                        'intensity.index': 'intensity_index'})
        
        logging.info(f"Datos procesados exitosamente")

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        logging.error(f"La petición a {base_url} ha fallado: {e}")
        return None

    except json.JSONDecodeError:
        # Registrar error de formato JSON
        logging.error(f"Respuesta en formato incorrecto de {base_url}")
        
    except Exception as e:
        # Registrar cualquier otro error
        logging.exception(f"Error al obtener datos de {base_url}: {e}")
    
    return df

def connect_to_db(config_file, section):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parameters:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.

    Returns:
    sqlalchemy.engine.base.Engine: Un objeto de conexión a la base de datos.
    """
    try:
        parser = ConfigParser()
        parser.read(config_file)

        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            logging.info("Conectándose a la base de datos...")
            engine = create_engine(
                f"{db['dbengine']}://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
                , connect_args={"options": f"-c search_path={db['schema']}"}
                )

            logging.info("Conexión a la base de datos establecida exitosamente")
            return engine

        else:
            logging.error(f"Sección {section} no encontrada en el archivo de configuración")
            return None
        
    except Exception as e:
        logging.error(f"Error al conectarse a la base de datos: {e}")
        return None

def load_to_sql(df, table_name, engine):
    """
    Cargar un dataframe en una tabla de base de datos,
    usando una tabla intermedia o stage para control de duplicados.

    Parameters:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
    """
    try:
        with engine.connect() as conn:
            logging.info(f"Cargando datos en la tabla {table_name}_stg...")
            
            conn.execute(f"TRUNCATE TABLE {table_name}_stg")
            
            df.to_sql(
                f"{table_name}_stg",
                conn,
                if_exists="append",
                method="multi",
                chunksize = 1000,
                index=False
                )
            
            logging.info(f"Datos cargados exitosamente")
            logging.info(f"Cargando datos en la tabla {table_name}...")
            
            conn.execute(f"""
                BEGIN;
                MERGE INTO {table_name}
                    USING {table_name}_stg AS stg
                    ON {table_name}.from_date = stg.from_date
                    AND {table_name}.to_date = stg.to_date
                    WHEN MATCHED THEN
                        UPDATE SET
                            intensity_max = stg.intensity_max,
                            intensity_average = stg.intensity_average,
                            intensity_min = stg.intensity_min,
                            intensity_index = stg.intensity_index
                    WHEN NOT MATCHED THEN
                        INSERT (from_date, to_date, intensity_max, intensity_average, intensity_min, intensity_index)
                        VALUES (stg.from_date, stg.to_date, stg.intensity_max, stg.intensity_average, stg.intensity_min, stg.intensity_index);
                COMMIT;
                """)
            
            logging.info("Datos cargados exitosamente")
    
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")

'''
def get_data(base_url, endpoint, params=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parametros:
    base_url: La URL base de la API.
    endpoint: El endpoint (ruta) de la API para obtener datos específicos.
    params: Los parámetros de la solicitud GET.
    
    Retorno:
    Un DataFrame con los datos obtenidos de la API.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        logging.info(f"Obteniendo datos de {endpoint_url}...")
        logging.info(f"Parámetros: {params}")
        response = requests.get(endpoint_url, params=params)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.
        logging.info(response.url)
        logging.info("Datos obtenidos exitosamente... Procesando datos...")
        data = response.json()
        data = data["data"]
        df = json_normalize(data)

        logging.info(f"Datos procesados exitosamente")

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        logging.error(f"La petición a {base_url} ha fallado: {e}")
        return None

    except json.JSONDecodeError:
        # Registrar error de formato JSON
        logging.error(f"Respuesta en formato incorrecto de {base_url}")
        
    except Exception as e:
        # Registrar cualquier otro error
        logging.exception(f"Error al obtener datos de {base_url}: {e}")
    
    return df
        

def load_to_sql(df, table_name, engine, check_field):
    """
    Cargar un dataframe en una tabla de base de datos,
    usando una tabla intermedia o stage para control de duplicados.

    Parameters:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
    check_field (str): El nombre de la columna que se usará para controlar duplicados.
    """
    try:
        with engine.connect() as conn:
            logging.info(f"Cargando datos en la tabla {table_name}_stg...")
            conn.execute(f"TRUNCATE TABLE {table_name}_stg")
            df.to_sql(
                f"{table_name}_stg", conn,
                if_exists="append", method="multi",
                index=False
                )
            logging.info(f"Datos cargados exitosamente")
            logging.info(f"Cargando datos en la tabla {table_name}...")
            conn.execute(f"""
                BEGIN;
                DELETE FROM {table_name}
                USING {table_name}_stg
                WHERE {table_name}.{check_field} = {table_name}_stg.{check_field};

                INSERT INTO {table_name}
                SELECT * FROM {table_name}_stg;
                COMMIT;
                """)
            logging.info("Datos cargados exitosamente")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")
'''