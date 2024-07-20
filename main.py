import spotipy
import wheel
import pandas as pd
import psycopg2
import json

from spotipy.oauth2 import SpotifyClientCredentials

with open("client_secret_spotify.txt",'r') as c:
    pwd= c.read()
    

client_id ='db11e97f789a4d48a39afcdda24fb2c0'
client_secret = pwd

#autentico spotiy
client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=pwd)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Realiza una consulta simple (prueba) a la API de Spotify
results = sp.search(q='artists:servando y florentino', type='album')

#imprimo el resultado
#print(result)


# Imprime la respuesta completa para verificar su estructura
print(json.dumps(results, indent=2))