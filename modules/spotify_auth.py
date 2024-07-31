import spotipy
from spotify_auth import SpotifyClientCredentials


#defino la conexion
def authenticate_spotify(client_id, client_secret):
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp

def read_spotify_secret(file_path):
    with open(file_path, 'r') as c:
        return c.read()