from config import SPOTIFY_CLIENT_ID, SPOTIFY_SECRET_FILE, REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DBNAME, REDSHIFT_USER, REDSHIFT_PASSWORD_FILE, YEARS
from utils import setup_logging, load_credentials
from spotify import authenticate_spotify, get_tracks_data, create_dataframe
from redshift import connect_redshift, create_table,insert_data,drop_table #truncate_table

def main():
    # Configuración del logging
    setup_logging()

    # Cargar credenciales
    client_secret = load_credentials(SPOTIFY_SECRET_FILE)
    password = load_credentials(REDSHIFT_PASSWORD_FILE)

    # Autenticación con Spotify
    sp = authenticate_spotify(SPOTIFY_CLIENT_ID, client_secret)

    # Conectar a Redshift
    conn = connect_redshift(REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DBNAME, REDSHIFT_USER, password)

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
