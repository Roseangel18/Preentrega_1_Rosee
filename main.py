import json
import pandas as pd
from modules.spotify_auth import authenticate_spotify, read_spotify_secret
from modules.redshift_connection import read_redshift_password, connect_redshift, create_canciones_table, truncate_canciones_table



def main():
    # Autenticación con Spotify
    client_id = 'db11e97f789a4d48a39afcdda24fb2c0'
    client_secret = read_spotify_secret("client_secret_spotify.txt")
    sp = authenticate_spotify(client_id, client_secret)
    
    # Realiza una consulta simple (prueba) a la API de Spotify
    # results = sp.search(q='artists:servando y florentino', type='track', limit=50)
    # print(json.dumps(results, indent=2))
    
    # Conexión a Redshift
    dbname = 'data-engineer-database'
    host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    port = '5439'
    username = 'roseangelbazan1_coderhouse'
    password = read_redshift_password("pwd_redshift.txt")
    conn = connect_redshift(dbname, host, port, username, password)
    
    if conn:
        create_canciones_table(conn)
        truncate_canciones_table(conn)
        conn.close()

if __name__ == "__main__":
    main()
