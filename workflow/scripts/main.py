from datetime import datetime, timedelta

import pandas as pd

from utils import *

base_url = "https://api.luchtmeetnet.nl/open_api"


def load_stations_data(config_file):
    """
    Proceso ETL para los datos de estaciones
    """

    df_stations_tmp = get_data(base_url, "stations", params={"organization_id": 1})

    # agregar excepcion por http error 429
    try:
        stations = []
        for station in df_stations_tmp.number.unique():
            df_station = get_data(base_url, f"stations/{station}")
            df_station["station"] = station
            stations.append(df_station)
        
        df_stations = pd.concat(stations)
        df_stations = df_stations[['station', 'type', 'municipality', 'url', 'province', 'organisation', 'location', 'year_start']]

        engine = connect_to_db(config_file, "redshift")
        load_to_sql(df_stations, "stations", engine, check_field="station")
    except Exception as e:
        logging.error(f"Error al obtener datos de {base_url}: {e}")
        raise e

def load_measurements_data(config_file, start, end):
    """
    Proceso ETL para los datos de mediciones
    """
    try:
        df_measurements = get_data(
            base_url, "measurements",
            params={"start": start, "end": end}
            )
        
        engine = connect_to_db(config_file, "redshift")
        load_to_sql(df_measurements, "measurements", engine, check_field="station_number")
    except Exception as e:
        logging.error(f"Error al obtener datos de {base_url}: {e}")
        raise e

if __name__ == "__main__":
    load_stations_data("config/config.ini")

    start = datetime.utcnow() - timedelta(hours=1)
    end = start.strftime("%Y-%m-%dT%H:59:59Z")
    start = start.strftime("%Y-%m-%dT%H:00:00Z")
    load_measurements_data("config/config.ini", start, end)