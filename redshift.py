import psycopg2
import logging
from psycopg2.extras import execute_values

def connect_redshift(host, port, dbname, user, password):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logging.info("Conexión exitosa a Redshift")
        return conn
    except Exception as e:
        logging.error(f"Error al conectar a Redshift: {e}")
        exit(1)

def drop_table(cur):
    try:
        cur.execute("""
           
            DROP TABLE IF EXISTS roseangelbazan1_coderhouse.canciones
            
            """)
        cur.connection.commit()
        logging.info("Tabla 'caciones' dropeada con exito!")
    except Exception as e:
        logging.error(f"Error al dropear la tabla {e}")
        cur.connetion.close()
        exit(1)
            

def create_table(cur):
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS canciones (
                id VARCHAR(50) PRIMARY KEY,
                artista VARCHAR(255),
                cancion VARCHAR(255),
                genero VARCHAR(300),
                album VARCHAR(100),
                total_canciones_album INTEGER,
                popularidad INTEGER,
                fecha_lanzamiento DATE,
                duracion_ms INTEGER,
                album_img VARCHAR(300),
                fecha_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.connection.commit()
        logging.info("Tabla 'canciones' creada OK")
    except Exception as e:
        logging.error(f"Error al crear la tabla: {e}")
        cur.connection.close()
        exit(1)
        
"""
def truncate_table(cur):
    try:
        cur.execute("TRUNCATE TABLE canciones")
        cur.connection.commit()
        logging.info("Tabla 'canciones' truncada")
    except Exception as e:
        logging.error(f"Error al truncar la tabla: {e}")
        cur.connection.close()
        exit(1)
"""        

def insert_data(cur, df):
    try:
        with cur.connection.cursor() as cur:
            execute_values(
                cur,
                '''
                INSERT INTO canciones (Id, Artista, Cancion, Duracion_ms, Genero, Album, Album_img, Total_canciones_album, Popularidad, fecha_lanzamiento)
                VALUES %s
                ''',
                [tuple(row) for row in df.values],
                page_size=len(df)
            )
            cur.connection.commit()
            num_records = len(df)
            logging.info(f"{num_records} registros insertados exitosamente en Redshift")
    except Exception as e:
        logging.error(f"Error al insertar datos en Redshift: {e}")