import spotipy
import wheel
import pandas as pd
import psycopg2
import json

#Conexion a la api de spotipy
from spotipy.oauth2 import SpotifyClientCredentials

with open("client_secret_spotify.txt",'r') as c:
    pwd= c.read()
    

client_id ='db11e97f789a4d48a39afcdda24fb2c0'
client_secret = pwd

#Conexion a Redsshift con mi user:

dbname= 'data-engineer-database'
host ='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'
username='roseangelbazan1_coderhouse'

with open("pwd_redshift.txt",'r') as f:
    pwd = f.read()

#creo el string de conexion

conn_string = (f"dbname='{dbname}' host='{host}' port='{port}' user='{username}' password='{pwd}'")

#Intento conectarme

try:
    conn = psycopg2.connect(conn_string)
    print("Conexion exitosa!!")

except Exception as e:
    print(f"Error al conectar a Redshift: {e}")













#autentico spotiy
#client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=pwd)
#sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Realiza una consulta simple (prueba) a la API de Spotify
#results = sp.search(q='artists:servando y florentino', type='album')

#imprimo el resultado
#print(results)


# Imprime la respuesta completa para verificar su estructura
##print(json.dumps(results, indent=2))