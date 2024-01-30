from configparser import ConfigParser
import psycopg2

#Read config file
config = ConfigParser()
config.read('config.cfg')

#Connection info
host = config.get('redshift', 'host')
database = config.get('redshift', 'database')
user = config.get('redshift', 'user')
password = config.get('redshift', 'password')
port = config.get('redshift', 'port')

#Connection
conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password,
    port=port
)

#Create cursor
cur = conn.cursor()

#Define SQL query
create_table_query = '''
CREATE TABLE IF NOT EXISTS Carbon_Intensity (
    Test1 INT,
    Test2 INT,
    Test3 INT
);
'''

# Ejecuta la sentencia SQL
cur.execute(create_table_query)

# Confirma los cambios
conn.commit()

# Cierra el cursor y la conexión
cur.close()
conn.close()