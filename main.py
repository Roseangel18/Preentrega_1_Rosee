import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import logging


# Configuración del logging para consola y archivo
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Crear un manejador de archivo
file_handler = logging.FileHandler('Ejecucion.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Crear un manejador de consola
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

def load_credentials(filename):
    with open(filename, 'r') as f:
        return f.read().strip()

# Cargar credenciales de Spotify
client_id = 'db11e97f789a4d48a39afcdda24fb2c0'
client_secret = load_credentials("client_secret_spotify.txt")

# Autenticación con Spotify
try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
    logging.info("Autenticación exitosa con Spotify")
except Exception as e:
    logging.error(f"Error en la autenticación con Spotify: {e}")
    exit(1)

# Datos de conexión a Redshift
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
dbname = 'data-engineer-database'
user = 'roseangelbazan1_coderhouse'
password = load_credentials("pwd_redshift.txt")

# Crear la conexión a Redshift
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    logging.info("Conexión exitosa a Redshift")
except Exception as e:
    logging.error(f"Error al conectar a Redshift: {e}")
    exit(1)

# Crear un cursor
try:
    cur = conn.cursor()
    logging.info("Cursor creado exitosamente")
except Exception as e:
    logging.error(f"Error al crear el cursor: {e}")
    conn.close()
    exit(1)

# Crear la tabla si no existe
try:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS roseangelbazan1_coderhouse.canciones (
            id VARCHAR(50) PRIMARY KEY,
            artista VARCHAR(255),
            cancion VARCHAR(255),
            genero VARCHAR(300),
            album VARCHAR(100),
            total_canciones_album INTEGER,
            popularidad INTEGER,
            fecha_lanzamiento DATE,
            duracion_ms INTEGER,
            album_img VARCHAR(300)
        )
    """)
    conn.commit()
    logging.info("Tabla 'canciones' creada o ya existe")
except Exception as e:
    logging.error(f"Error al crear la tabla: {e}")
    conn.close()
    exit(1)

# Truncar la tabla
try:
    cur.execute("TRUNCATE TABLE roseangelbazan1_coderhouse.canciones")
    conn.commit()
    logging.info("Tabla 'canciones' truncada")
except Exception as e:
    logging.error(f"Error al truncar la tabla: {e}")
    conn.close()
    exit(1)

# Definir los años que deseas buscar
years = [2022, 2023, 2024]

# Lista para almacenar los datos
data = []

# Buscar pistas para cada año
for year in years:
    try:
        results = sp.search(q=f'year:{year}', type='track', limit=50)
        for track in results['tracks']['items']:
            id = track['id']
            artist_name = track['artists'][0]['name']
            artist_id = track['artists'][0]['id']
            track_name = track['name'].replace("'", "")  # Quitar las comillas simples
            duration_ms = track['duration_ms']
            album_name = track['album']['name'].replace("'", "")  # Quitar las comillas simples
            album_img = track['album']['images'][0]['url']  # Imagen del álbum
            album_total_tracks = track['album']['total_tracks']
            track_genres = ', '.join(sp.artist(artist_id)['genres'])  # Separar el género por coma
            track_popularity = track['popularity']
            release_date = track['album']['release_date']

            # Agregar los datos a la lista
            data.append([id, artist_name, track_name, duration_ms, track_genres, album_name, album_img, album_total_tracks, track_popularity, release_date])
        logging.info(f"Datos obtenidos para el año {year}")
    except Exception as e:
        logging.error(f"Error al buscar pistas para el año {year}: {e}")

# Crear el DataFrame
try:
    df = pd.DataFrame(data, columns=['Id', 'Artista', 'Cancion', 'Duracion_ms', 'Genero', 'Album', 'Album_img', 'Total_canciones_album', 'Popularidad', 'fecha_lanzamiento'])
    logging.info("DataFrame creado exitosamente")
except Exception as e:
    logging.error(f"Error al crear el DataFrame: {e}")

# Evitar canciones duplicadas
df = df.drop_duplicates(subset=['Artista', 'Cancion', 'Album'], keep='first')

# Reemplazar valores nulos o vacíos en el campo 'Genero' por 'Desconocido'
df['Genero'] = df['Genero'].replace('', 'Desconocido').fillna('Desconocido')

# Evitar canciones con duración 0 ms
df = df[df['Duracion_ms'] != 0]

# Convertir 'fecha_lanzamiento' a formato de fecha
df['fecha_lanzamiento'] = pd.to_datetime(df['fecha_lanzamiento'], errors='coerce')

# Mostrar el DataFrame
#print(df)

# Insertar datos en la tabla de Redshift
try:
    with conn.cursor() as cur:
        execute_values(
            cur,
            '''
            INSERT INTO canciones (Id, Artista, Cancion, Duracion_ms, Genero, Album, Album_img, Total_canciones_album, Popularidad, fecha_lanzamiento)
            VALUES %s
            ''',
            [tuple(row) for row in df.values],
            page_size=len(df)
        )
        conn.commit()
        logging.info("Datos insertados exitosamente en Redshift")
except Exception as e:
    logging.error(f"Error al insertar datos en Redshift: {e}")
finally:
    # Cerrar el cursor y la conexión
    cur.close()
    conn.close()
    logging.info("Conexión a Redshift cerrada")