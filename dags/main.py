from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME, POSTGRES_USER, YEARS
from utils import setup_logging, load_credentials
from spotify import authenticate_spotify, get_tracks_data, create_dataframe
from redshift import connect_postgres, create_table,insert_data,drop_table #truncate_table
from dotenv import load_dotenv
import os

# Cargar las variables del archivo .env
load_dotenv()



def main():
    # Configuración del logging
    setup_logging()
    

    # Cargar credenciales
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    password = os.getenv("POSTGRES_PASSWORD")

    # Autenticación con Spotify
    sp = authenticate_spotify(client_id,client_secret)

    # Conectar a connect_postgres
    #print("Password:", password)
    conn = connect_postgres(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME, POSTGRES_USER, password)

    # Crear un cursor
    cur = conn.cursor()

    # dropear y Crear la tabla si no existe y truncarla
    drop_table(cur)
    create_table(cur)
    #truncate_table(cur)

    # Obtener datos de pistas de Spotify
    data = get_tracks_data(sp, YEARS)

    # Crear DataFrame y limpiar datos
    df = create_dataframe(data)

    # Insertar datos en Redshift
    insert_data(cur, df)

    # Cerrar el cursor y la conexión
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()